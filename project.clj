(defproject org.zeromq/cljzmq "0.1.1"
  :description "Clojure binding for ØMQ"
  :url "https://github.com/zeromq/cljzmq"
  :license {:name "LGPLv3+"
            :url "http://www.gnu.org/licenses/lgpl.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.zeromq/jzmq "2.1.2"]]
  :codox {:src-dir-uri "http://github.com/zeromq/cljzmq/blob/master"
          :src-linenum-anchor-prefix "L"}
  :profiles
  {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
   :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}}
  :aliases {"all" ["with-profile" "dev:1.3:1.4"]}
  :repositories [["releases" {:url "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
                              :username [:gpg :env/NEXUS_USERNAME]
                              :password [:gpg :env/NEXUS_PASSWORD]}]
                 ["snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                               :username [:gpg :env/NEXUS_USERNAME]
                               :password [:gpg :env/NEXUS_PASSWORD]
                               :update :always}]]
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

