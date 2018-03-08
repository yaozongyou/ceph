// Copyright (c) 2017. All rights reserved.

#ifndef COMMON_MISC_COUNTER_H
#define COMMON_MISC_COUNTER_H

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>
//#include "common/system/concurrency/atomic/atomic.h"
//#include "common/system/concurrency/thread_pool.h"
//#include "common/system/timer/timer_manager.h"

#define COUNTER_frequency(name, desc) \
    static common::FrequencyCounter FREQUENCY_##name(#name, #desc);   \
    static common::CounterRegisterer registerer_frequency_##name(#name, &FREQUENCY_##name)

#define COUNTER_latency(name, desc) \
    static common::LatencyCounter LATENCY_##name(#name, #desc);     \
    static common::CounterRegisterer registerer_latency_##name(#name, &LATENCY_##name)

#define COUNTER_distribution(name, desc) \
    static common::DistributionCounter DISTRIBUTION_##name(#name, #desc);     \
    static common::CounterRegisterer registerer_distribution_##name(#name, &DISTRIBUTION_##name)

namespace common {

class CounterManager;

class FrequencyCounter {
public:
    FrequencyCounter(const std::string& name, const std::string& desc);
    ~FrequencyCounter();

    void Add();
    void Add(uint32_t times);

private:
    friend CounterManager;
    uint32_t Reset();

    std::string name_;
    std::string desc_;
    std::atomic<uint32_t> times_;
};

class LatencyCounter {
public:
    LatencyCounter(const std::string& name, const std::string& desc);
    ~LatencyCounter();

    void Add(uint32_t latency_ms);

private:
    friend CounterManager;

    struct LatencyInfo {
        uint32_t max_latency_ms;
        uint32_t min_latency_ms;
        uint32_t total_latency_ms;
        uint32_t total_count;
    };

    LatencyInfo Reset();

    std::string name_;
    std::string desc_;
    LatencyInfo latency_info_;
    std::mutex mutex_;
};

class ScopedLatency {
public:
    explicit ScopedLatency(LatencyCounter* counter);
    ~ScopedLatency();

private:
    LatencyCounter* counter_;
    int64_t timestamp_ms_;
};

class DistributionCounter {
public:
    DistributionCounter(const std::string& name, const std::string& desc);
    ~DistributionCounter();

    void Add(int32_t key);

private:
    friend CounterManager;
    typedef std::map<int32_t, uint32_t> DistributionInfo;

    DistributionInfo Reset();

    std::string name_;
    std::string desc_;
    DistributionInfo distribution_info_;
    std::mutex mutex_;
};

class CounterRegisterer {
public:
    template <class Counter>
    CounterRegisterer(const std::string& name, Counter* counter);
};

class CounterManager {
public:
    static CounterManager* GlobalCounterManager();   // returns a singleton manager.
    static void DeleteGlobalCounterManager();

    void InitCounterManager();
    void RegisterCounter(const std::string& name, FrequencyCounter* counter);
    void RegisterCounter(const std::string& name, LatencyCounter* counter);
    void RegisterCounter(const std::string& name, DistributionCounter* counter);

private:
    CounterManager();
    CounterManager(const CounterManager&);  // no copying!
    void operator=(const CounterManager&);
    ~CounterManager();

    void ThreadMain();
    void OutputCounterWrapper(uint64_t timer_id);
    void OutputCounter();

    std::map<std::string, FrequencyCounter*> frequency_counters_;
    std::map<std::string, LatencyCounter*> latency_counters_;
    std::map<std::string, DistributionCounter*> distribution_counters_;

    std::mutex mutex_;
    std::atomic<bool> quit_;
    std::condition_variable cv_;
    std::mutex cv_m_;
    std::thread t_;
    static CounterManager* global_counter_manager_;
    static std::mutex global_counter_manager_mutex_;
};

void InitCounterManager();
void ShutDownCounterManager();

} // namespace common

#endif // COMMON_MISC_COUNTER_H
