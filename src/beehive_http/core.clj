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
  (:import (com.ning.http.client AsyncHttpClient RequestBuilder)
           (com.ning.http.client.providers.netty.response NettyResponse)
           (net.uncontended.precipice ServiceProperties RejectedActionException)
           (net.uncontended.precipice_implementations.asynchttp HttpAsyncService)))

(set! *warn-on-reflection* true)

(defn async-http-client []
  (AsyncHttpClient.))

(defn request [url]
  (.build (doto (RequestBuilder.)
            (.setUrl ^String url)
            (.setRequestTimeout 500))))

(defn response [^NettyResponse response]
  {:status (.getStatusCode response)
   :headers (.getHeaders response)
   :body (.getResponseBodyAsStream response)})

(defn service [name]
  (let [properties (ServiceProperties.)]
    (HttpAsyncService. name properties (async-http-client))))

(defn execute [service request]
  (try
    (f/->CLJResilientFuture
      (.submitRequest ^HttpAsyncService service request))
    (catch RejectedActionException e
      (f/->CLJRejectedFuture (.reason e)))))