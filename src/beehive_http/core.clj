;; Copyright 2015 Timothy Brooks
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns beehive-http.core
  (:require [beehive.future :as f])
  (:import (com.ning.http.client AsyncHttpClient
                                 RequestBuilder
                                 AsyncHttpProvider
                                 AsyncHttpClientConfig$Builder)
           (com.ning.http.client.providers.netty NettyAsyncHttpProviderConfig
                                                 NettyAsyncHttpProvider)
           (com.ning.http.client.providers.netty.response NettyResponse)
           (net.uncontended.precipice ServiceProperties
                                      RejectedActionException)
           (net.uncontended.precipice_implementations.asynchttp HttpAsyncService
                                                                Transformer)
           (java.util.concurrent TimeUnit)
           (java.util Map)
           (org.jboss.netty.util HashedWheelTimer)))

(set! *warn-on-reflection* true)

(defn parse-response [^NettyResponse response]
  {:status (.getStatusCode response)
   :headers (.getHeaders response)
   :body (.getResponseBodyAsStream response)})

(def response-transformer
  (reify Transformer
    (transform [_ response]
      (parse-response response))))

(defn async-http-client [& {:keys [socket-timeout]}]
  (let [timer (HashedWheelTimer. 20 TimeUnit/MILLISECONDS)
        netty-config (doto (NettyAsyncHttpProviderConfig.)
                       (.setNettyTimer timer))
        client-config (.build
                        (doto (AsyncHttpClientConfig$Builder.)
                          (.setAsyncHttpClientProviderConfig netty-config)
                          (.setConnectTimeout (int (or socket-timeout 15)))
                          (.setReadTimeout (int (or socket-timeout 15)))))
        provider (NettyAsyncHttpProvider. client-config)]
    (AsyncHttpClient. ^AsyncHttpProvider provider)))

(defn format-headers [headers]
  (into {} (mapv (fn [[k v]]
                   [k (if (sequential? v) v (vector v))])
                 headers)))

(defn request [url & {:keys [timeout headers body method]}]
  (let [builder (RequestBuilder.)]
    (.setUrl builder ^String url)
    (when method (.setMethod builder ^String method))
    (when headers (.setHeaders builder ^Map (format-headers headers)))
    (when body (.setBody builder ^bytes body))
    (when timeout (.setRequestTimeout builder (int timeout)))
    (.build builder)))

(defn service [name & http-configs]
  (let [properties (ServiceProperties.)]
    (HttpAsyncService. name properties (apply async-http-client http-configs))))

(defn execute [^HttpAsyncService service request]
  (try
    (f/->BeehiveFuture
      (.submitRequest service request response-transformer))
    (catch RejectedActionException e
      (f/->BeehiveRejectedFuture (.reason e)))))