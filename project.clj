(defproject org.zeromq/cljzmq "0.1.5-SNAPSHOT"
  :description "Clojure binding for ØMQ"
  :url "https://github.com/zeromq/cljzmq"
  :license {:name "LGPLv3+"
            :url "http://www.gnu.org/licenses/lgpl.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.zeromq/jzmq "3.0.1"]]
  :codox {:src-dir-uri "http://github.com/zeromq/cljzmq/blob/master"
          :src-linenum-anchor-prefix "L"}
  :profiles
  {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
   :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
   :1.5.1 {:dependencies [[org.clojure/clojure "1.5.1"]]}}
  :aliases {"all" ["with-profile" "dev:1.3:1.4:1.5.1"]}
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
                  [:email "trevor.bernard@gmail.com"]]
                 [:developer
                  [:name "Josh Comer"]
                  [:email "jcomer@liveops.com"]]
                 [:developer
                  [:name "Ian Bishop"]
                  [:email "ibishop@liveops.com"]]]
  :min-lein-version "2.0.0")
