// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_s3_client.h"
#include <cstring>
#include <iostream>
#include <openssl/hmac.h>
#include "rgw/rgw_b64.h"

RGWS3Client::RGWS3Client(
    const std::string& rgw_address, const std::string& access_key, const std::string& secret_key) 
    : rgw_address_(rgw_address), access_key_(access_key), secret_key_(secret_key) {
  curl_ = curl_easy_init();
  assert(curl_ != NULL);
}

RGWS3Client::~RGWS3Client() {
  curl_easy_cleanup(curl_);
}
  
static size_t CurlReadCallbackWrapper(char *buffer, size_t size, size_t nitems, void *instream) {
  if (instream == nullptr) {
    return 0;
  }
  std::string_view* content = static_cast<std::string_view*>(instream);
  std::size_t length = std::min(size * nitems, content->size());
  memcpy(buffer, content->data(), length);
  content->remove_prefix(length);
  return length;
}

static size_t CurlWriteCallbackWrapper(char *ptr, size_t size, size_t nmemb, void *userdata) {
  if (userdata != nullptr) {
    (static_cast<std::string*>(userdata))->append(ptr, size * nmemb);
  }
  return size * nmemb;
}

bool RGWS3Client::create_bucket(const std::string& bucket) {
  std::string date = get_date();
  std::string string_to_sign =
      "PUT\n"
      "\n" 
      "\n" 
      + date + "\n" 
      + "/" + bucket;
  std::string signature = "AWS " + access_key_ + ":" +
      create_signature(string_to_sign);
  struct curl_slist* header = curl_slist_append(nullptr, ("Authorization: " + signature).c_str());
  header = curl_slist_append(header, ("Date: " + date).c_str());

  reset();

  std::string url = "http://" + rgw_address_ + "/" + bucket;
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header);
  curl_easy_setopt(curl_, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl_, CURLOPT_INFILESIZE_LARGE, 0);
  curl_easy_setopt(curl_, CURLOPT_READFUNCTION, CurlReadCallbackWrapper);
  curl_easy_setopt(curl_, CURLOPT_READDATA, nullptr);
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, CurlWriteCallbackWrapper);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, nullptr);
  CURLcode curl_code = curl_easy_perform(curl_);
  curl_slist_free_all(header);

  if (curl_code != CURLE_OK) {
    std::cerr << "curl_easy_perform failed: curl_code " << curl_code
        << " curl_message " << curl_easy_strerror(curl_code) << std::endl;
    return false;
  }

  long http_status_code = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_status_code);
  if (http_status_code != 200) {
    std::cerr << "failed to create bucket http_status_code: " << http_status_code << std::endl;
    return false;
  }

  return true;
}
  
bool RGWS3Client::remove_bucket(const std::string& bucket) {
  std::string date = get_date();
  std::string string_to_sign =
        "DELETE\n"
        "\n" 
        "\n" 
        + date + "\n" 
        + "/" + bucket;
  std::string signature = "AWS " + access_key_ + ":" + create_signature(string_to_sign);
  struct curl_slist* header = curl_slist_append(nullptr, ("Authorization: " + signature).c_str());
  header = curl_slist_append(header, ("Date: " + date).c_str());

  reset();

  std::string url = "http://" + rgw_address_ + "/" + bucket;
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header);
  curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "DELETE");
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, CurlWriteCallbackWrapper);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, nullptr);
  CURLcode curl_code = curl_easy_perform(curl_);
  curl_slist_free_all(header);

  if (curl_code != CURLE_OK) {
    std::cerr << "curl_easy_perform failed: curl_code " << curl_code
        << " curl_message " << curl_easy_strerror(curl_code) << std::endl;
    return false;
  }

  long http_status_code = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_status_code);
  if (http_status_code != 204) {
    std::cerr << "failed to remove bucket http_status_code: " << http_status_code << std::endl;
    return false; 
  }

  return true;
}

bool RGWS3Client::put_object(
    const std::string& bucket, const std::string& key, uint64_t content_length,
    read_callback_t* callback, void* user_data) {
  std::string date = get_date();
  std::string string_to_sign =
      "PUT\n"
      "\n"
      "\n"
      + date + "\n"
      + "/" + bucket + "/" + key;
  std::string signature = "AWS " + access_key_ + ":" + create_signature(string_to_sign);
  struct curl_slist* header = curl_slist_append(nullptr, ("Authorization: " + signature).c_str());
  header = curl_slist_append(header, ("Date: " + date).c_str());

  reset();

  std::string url = "http://" + rgw_address_ + "/" + bucket + "/" + key;
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header);
  curl_easy_setopt(curl_, CURLOPT_UPLOAD, 1L);
  curl_easy_setopt(curl_, CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(content_length));
  curl_easy_setopt(curl_, CURLOPT_READFUNCTION, callback);
  curl_easy_setopt(curl_, CURLOPT_READDATA, user_data);
  CURLcode curl_code = curl_easy_perform(curl_);
  curl_slist_free_all(header);

  if (curl_code != CURLE_OK) {
    std::cerr << "curl_easy_perform failed: curl_code " << curl_code
        << " curl_message " << curl_easy_strerror(curl_code) << std::endl;
    return false;
  }

  long http_status_code = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_status_code);
  if (http_status_code != 200) {
    std::cerr << "failed to put object http_status_code: " << http_status_code << std::endl;
    return false;
  }

  return true;
}
  
bool RGWS3Client::delete_object(const std::string& bucket, const std::string& key) {
  std::string date = get_date();
  std::string string_to_sign =
    "DELETE\n"
    "\n"
    "\n"
    + date + "\n"
    + "/" + bucket + "/" + key;
  std::string signature = "AWS " + access_key_ + ":" + create_signature(string_to_sign);
  struct curl_slist* header = curl_slist_append(nullptr, ("Authorization: " + signature).c_str());
  header = curl_slist_append(header, ("Date: " + date).c_str());

  reset();

  std::string url = "http://" + rgw_address_ + "/" + bucket + "/" + key;
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header);
  curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "DELETE");
  curl_easy_setopt(curl_, CURLOPT_NOBODY, 1L);
  CURLcode curl_code = curl_easy_perform(curl_);
  curl_slist_free_all(header);

  if (curl_code != CURLE_OK) {
    std::cerr << "curl_easy_perform failed: curl_code " << curl_code 
        << " curl_message " << curl_easy_strerror(curl_code) << std::endl;
    return -1;
  }

  long http_status_code = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_status_code);
  if (http_status_code != 204) {
    std::cerr << "failed to delete object http_status_code: " << http_status_code << std::endl;
    return false;
  }

  return 0;
}
  
bool RGWS3Client::get_object(const std::string& bucket, const std::string& key, 
    write_callback_t* callback, void* user_data) {
  std::string date = get_date();
  std::string string_to_sign =
      "GET\n"
      "\n"
      "\n"
      + date + "\n"
      + "/" + bucket + "/" + key;
  std::string signature = "AWS " + access_key_ + ":" + create_signature(string_to_sign);
  struct curl_slist* header = curl_slist_append(nullptr, ("Authorization: " + signature).c_str());
  header = curl_slist_append(header, ("Date: " + date).c_str());

  reset();
  
  std::string url = "http://" + rgw_address_ + "/" + bucket + "/" + key;
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header);
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, callback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, user_data);

  CURLcode curl_code = curl_easy_perform(curl_);
  curl_slist_free_all(header);

  if (curl_code != CURLE_OK) {
    std::cerr << "curl_easy_perform failed: curl_code " << curl_code
        << " curl_message " << curl_easy_strerror(curl_code) << std::endl;
    return false;
  }

  long http_status_code = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_status_code);
  if (http_status_code != 200) {
    std::cerr << "failed to get object http_status_code: " << http_status_code << std::endl;
    return false;
  }

  return true;
}
  
bool RGWS3Client::list_objects(
    const std::string& bucket,
    const std::string& delimiter,
    const std::string& marker,
    int max_keys,
    const std::string& prefix,
    bool* is_truncated,
    std::string* next_marker,
    std::vector<std::string>* objects,
    std::vector<std::string>* dirs) {
  std::string query_string;
  if (!delimiter.empty()) {
    if (query_string.empty()) {
      query_string += "delimiter=" + delimiter;
    } else {
      query_string += "&delimiter=" + delimiter;
    }
  }
  if (!marker.empty()) {
    if (query_string.empty()) {
      query_string += "marker=" + marker;
    } else {
      query_string += "&marker=" + marker;
    }
  }
  if (max_keys != 1000) {
    if (query_string.empty()) {
      query_string += "max-keys=" + std::to_string(max_keys);
    } else {
      query_string += "&max-keys=" + std::to_string(max_keys);
    }
  }
  if (!prefix.empty()) {
    if (query_string.empty()) {
      query_string += "prefix=" + prefix;
    } else {
      query_string += "&prefix=" + prefix;
    }
  }

  std::string date = get_date();
  std::string string_to_sign =
      "GET\n"
      "\n"
      "\n" +
      date + "\n" +
      "/" + bucket + "/";
  std::string signature = "AWS " + access_key_ + ":" + create_signature(string_to_sign);
  struct curl_slist* header = curl_slist_append(nullptr, ("Authorization: " + signature).c_str());
  header = curl_slist_append(header, ("Date: " + date).c_str());

  reset();

  std::string response_body;
  std::string url = "http://" + rgw_address_ + "/" + bucket + "/" + (query_string.empty() ? "" : ("?" + query_string));
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, header);
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, CurlWriteCallbackWrapper);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_body);
  CURLcode curl_code = curl_easy_perform(curl_);
  curl_slist_free_all(header);

  if (curl_code != CURLE_OK) {
    std::cerr << "curl_easy_perform failed: curl_code " << curl_code
        << " curl_message " << curl_easy_strerror(curl_code) << std::endl;
    return false;
  }

  long http_status_code = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_status_code);
  if (http_status_code != 200) {
    std::cerr << "list objects failed: http_status_code " << http_status_code << std::endl;
    return false;
  }

  /*
    rapidxml::xml_document<> doc;
    doc.parse<0>(const_cast<char*>(response_body_.c_str()));
    rapidxml::xml_node<>* root = doc.first_node();

    rapidxml::xml_node<>* node = root->first_node("IsTruncated");
    if ((node != NULL) && (strcmp(node->value(), "true") == 0)) {
        *is_truncated = true;
    } else {
        *is_truncated = false;
    }
    node = root->first_node("NextMarker");
    if (node != NULL) {
        *next_marker = node->value();
    }
    node = root->first_node("Contents");



    while (node != NULL) {
        ObjectInfo object_info;
        object_info.key = node->first_node("Key")->value();
        object_info.last_modified = node->first_node("LastModified")->value();
        object_info.etag = node->first_node("ETag")->value();
        object_info.size = node->first_node("Size")->value();
        object_info.storage_class = node->first_node("StorageClass")->value();
        object_info.owner_id = node->first_node("Owner")->first_node("ID")->value();
        object_info.owner_display_name = node->first_node("Owner")->first_node("DisplayName")->value();

        objects->push_back(object_info);

        node = node->next_sibling("Contents");
    }


    node = root->first_node("CommonPrefixes");
    while (node != NULL) {
        std::string dir = node->first_node("Prefix")->value();
        dirs->push_back(dir);
        node = node->next_sibling("CommonPrefixes");
    }
    */

  return true;
}

void RGWS3Client::reset() {
  curl_easy_reset(curl_);
}

std::string RGWS3Client::get_date() {
  time_t timestamp = time(NULL);
  struct tm tm = {0};
  gmtime_r(&timestamp, &tm);
  char date[128] = {0};
  strftime(date, sizeof(date), "%a, %d %b %Y %H:%M:%S %Z", &tm);
  return std::string(date);
}
  
std::string RGWS3Client::create_signature(const std::string& string_to_sign) {
  unsigned char md[EVP_MAX_MD_SIZE] = {0};
  unsigned int md_len = EVP_MAX_MD_SIZE;
  HMAC(EVP_sha1(), secret_key_.data(), secret_key_.size(),
      reinterpret_cast<const unsigned char*>(string_to_sign.data()), string_to_sign.size(), 
      md, &md_len);
  std::string mac(reinterpret_cast<const char*>(md), static_cast<size_t>(md_len));
  return rgw::to_base64(mac);
}
