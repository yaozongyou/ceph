// Copyright (c) 2017. All rights reserved.

#include "counter.h"
#include <chrono>
#include <cstring>
#include <iostream>
#include <sstream>
#include <sys/time.h>
#include "common/log_message.h"

using namespace std::chrono_literals;

namespace common {

FrequencyCounter::FrequencyCounter(const std::string& name, const std::string& desc) :
    name_(name), desc_(desc), times_(0) {
}

FrequencyCounter::~FrequencyCounter() {
}

void FrequencyCounter::Add() {
    Add(1);
}

void FrequencyCounter::Add(uint32_t times) {
    times_ += times;
}

uint32_t FrequencyCounter::Reset() {
    return times_.exchange(0);
}

template <>
CounterRegisterer::CounterRegisterer(const std::string& name, FrequencyCounter* counter) {
    CounterManager::GlobalCounterManager()->RegisterCounter(name, counter);
}

LatencyCounter::LatencyCounter(const std::string& name, const std::string& desc) :
    name_(name), desc_(desc) {
    memset(&latency_info_, 0, sizeof(latency_info_));
}

LatencyCounter::~LatencyCounter() {
}

void LatencyCounter::Add(uint32_t latency_ms) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (latency_info_.total_count == 0) {
        latency_info_.max_latency_ms = latency_ms;
        latency_info_.min_latency_ms = latency_ms;
    } else {
        latency_info_.max_latency_ms = std::max(latency_ms, latency_info_.max_latency_ms);
        latency_info_.min_latency_ms = std::min(latency_ms, latency_info_.min_latency_ms);
    }
    latency_info_.total_latency_ms += latency_ms;
    latency_info_.total_count++;
}

LatencyCounter::LatencyInfo LatencyCounter::Reset() {
    std::lock_guard<std::mutex> guard(mutex_);
    LatencyInfo latency_info = latency_info_;
    memset(&latency_info_, 0, sizeof(latency_info_));
    return latency_info;
}

template <>
CounterRegisterer::CounterRegisterer(const std::string& name, LatencyCounter* counter) {
    CounterManager::GlobalCounterManager()->RegisterCounter(name, counter);
}

ScopedLatency::ScopedLatency(LatencyCounter* counter) :
    counter_(counter) {
    struct timeval tv = {0};
    gettimeofday(&tv, NULL);
    timestamp_ms_ = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000;
}

ScopedLatency::~ScopedLatency() {
    struct timeval tv = {0};
    gettimeofday(&tv, NULL);
    int64_t timestamp_ms = (tv.tv_sec) * 1000 + (tv.tv_usec) / 1000;
    counter_->Add(timestamp_ms - timestamp_ms_);
}

DistributionCounter::DistributionCounter(const std::string& name, const std::string& desc) :
    name_(name), desc_(desc) {
}

DistributionCounter::~DistributionCounter() {
}

void DistributionCounter::Add(int32_t key) {
    std::lock_guard<std::mutex> guard(mutex_);

    DistributionInfo::iterator iter = distribution_info_.find(key);
    if (iter == distribution_info_.end()) {
        distribution_info_[key] = 1;
    } else {
        distribution_info_[key]++;
    }
}

DistributionCounter::DistributionInfo DistributionCounter::Reset() {
    std::lock_guard<std::mutex> guard(mutex_);

    DistributionInfo distribution_info;
    distribution_info_.swap(distribution_info);
    return distribution_info;
}

template <>
CounterRegisterer::CounterRegisterer(const std::string& name, DistributionCounter* counter) {
    CounterManager::GlobalCounterManager()->RegisterCounter(name, counter);
}

CounterManager* CounterManager::global_counter_manager_ = NULL;
std::mutex CounterManager::global_counter_manager_mutex_;
CounterManager::CounterManager() : quit_(false), t_(&CounterManager::ThreadMain, this) {
}

CounterManager::~CounterManager() {
    {	
        std::lock_guard<std::mutex> l(cv_m_);
        quit_ = true;	
    }
    cv_.notify_one();	
    t_.join();
    OutputCounter();
}

CounterManager* CounterManager::GlobalCounterManager() {
    std::lock_guard<std::mutex> guard(CounterManager::global_counter_manager_mutex_);
    if (global_counter_manager_ == NULL) {
        global_counter_manager_ = new CounterManager();
    }

    return global_counter_manager_;
}

void CounterManager::DeleteGlobalCounterManager() {
    delete global_counter_manager_;
    global_counter_manager_ = NULL;
}

void CounterManager::InitCounterManager() {
}

void CounterManager::RegisterCounter(const std::string& name, FrequencyCounter* counter) {
    std::lock_guard<std::mutex> guard(mutex_);
    frequency_counters_[name] = counter;
}

void CounterManager::RegisterCounter(const std::string& name, LatencyCounter* counter) {
    std::lock_guard<std::mutex> guard(mutex_);
    latency_counters_[name] = counter;
}

void CounterManager::RegisterCounter(const std::string& name, DistributionCounter* counter) {
    std::lock_guard<std::mutex> guard(mutex_);
    distribution_counters_[name] = counter;
}

void CounterManager::ThreadMain() {
    for (;;) {
        std::unique_lock<std::mutex> lk(cv_m_);
        if (cv_.wait_for(lk, 60 * 1000ms, [this]{return this->quit_.load();})) {
	    break;
	} else {
	    OutputCounter();
	}
    }
}

void CounterManager::OutputCounter() {
    std::lock_guard<std::mutex> guard(mutex_);

    std::stringstream frequency_ss;
    for (std::map<std::string, FrequencyCounter*>::const_iterator iter = frequency_counters_.begin();
            iter != frequency_counters_.end(); ++iter) {
        const std::string& name = iter->first;
        uint32_t times = iter->second->Reset();

        frequency_ss << name << "=" << times << "&";
    }

    std::string str = frequency_ss.str();
    if (!str.empty()) {
        str = str.substr(0, str.size() - 1);
    }
    LOG(INFO) << "FrequencyCounter " << str;

    std::stringstream latency_ss;
    for (std::map<std::string, LatencyCounter*>::const_iterator iter = latency_counters_.begin();
            iter != latency_counters_.end(); ++iter) {
        const std::string& name = iter->first;
        LatencyCounter::LatencyInfo latency_info = iter->second->Reset();

        latency_ss << name << "_max_ms=" << latency_info.max_latency_ms << "#"
            << name << "_min_ms=" << latency_info.min_latency_ms << "#"
            << name << "_avg_ms=" << (latency_info.total_count == 0 ?
                    0 : latency_info.total_latency_ms / latency_info.total_count)
            << "&";
    }

    str = latency_ss.str();
    if (!str.empty()) {
        str = str.substr(0, str.size() - 1);
    }
    LOG(INFO) << "LatencyCounter " << str;

    std::stringstream distribution_ss;
    for (std::map<std::string, DistributionCounter*>::const_iterator iter = distribution_counters_.begin();
            iter != distribution_counters_.end(); ++iter) {
        const std::string& name = iter->first;
        DistributionCounter::DistributionInfo distribution_info = iter->second->Reset();

        distribution_ss << name << " ";
        for (DistributionCounter::DistributionInfo::const_iterator j = distribution_info.begin();
                j != distribution_info.end(); ++j) {
            int32_t key = j->first;
            uint32_t times = j->second;
            distribution_ss << key << "=" << times << " ";
        }
    }

    str = distribution_ss.str();
    if (!str.empty()) {
        str = str.substr(0, str.size() - 1);
    }
    LOG(INFO) << "DistributionCounter " << str;
}

void InitCounterManager() {
    CounterManager::GlobalCounterManager()->InitCounterManager();
}

void ShutDownCounterManager() {
    CounterManager::GlobalCounterManager()->DeleteGlobalCounterManager();
}

} // namespace common
