(defproject org.zeromq/cljzmq "0.1.0-SNAPSHOT"
  :description "Clojure binding for ØMQ"
  :url "https://github.com/zeromq/cljzmq"
  :license {:name "LGPLv3+"
            :url "http://www.gnu.org/licenses/lgpl.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.zeromq/jzmq "2.1.2"]]
  :pom-addition [:developers
                 [:developer
                  [:name "Trevor Bernard"]
                  [:email "trevor@userevents.com"]]
                 [:developer
                  [:name "Josh Comer"]
                  [:email "josh@userevents.com"]]
                 [:developer
                  [:name "Ian Bishop"]
                  [:email "ian@userevents.com"]]]
  :min-lein-version "2.0.0")

