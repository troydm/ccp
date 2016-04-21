(ns ccp.core-test
  (:require [clojure.test :refer :all]
            [ccp.core :as p]))

(deftest string-parse-stream-test
  (testing "String parse stream"
           (let [s (p/parse-stream "Hello!")
                 state (p/get-state s)]
             (is (= (p/next-elem s) \H))
             (is (= (do (p/reset s) (p/next-elem s)) \H))
             (is (= (p/next-elem s) \e))
             (is (= (p/prev-elem s) \H))
             (is (= (p/prev-elem s) nil))
             (is (= (p/next-elem s) \H))
             (is (= (p/current-elem s) \H))
             (is (= (do (p/next-elem s)
                      (p/next-elem s)
                      (p/next-elem s)
                      (p/next-elem s)
                      (p/next-elem s)
                      (p/next-elem s))
                    nil))
             (is (= (do (p/restore-state s state) (p/next-elem s)) \H)))))

(deftest sequence-parse-stream-test
  (testing "Sequence parse stream"
           (let [s (p/parse-stream [\H \e \l \l \o \!])
                 state (p/get-state s)]
             (is (= (p/next-elem s) \H))
             (is (= (do (p/reset s) (p/next-elem s)) \H))
             (is (= (p/next-elem s) \e))
             (is (= (p/prev-elem s) \H))
             (is (= (p/prev-elem s) nil))
             (is (= (p/next-elem s) \H))
             (is (= (p/current-elem s) \H))
             (is (= (do (p/next-elem s)
                      (p/next-elem s)
                      (p/next-elem s)
                      (p/next-elem s)
                      (p/next-elem s)
                      (p/next-elem s))
                    nil))
             (is (= (do (p/restore-state s state) (p/next-elem s)) \H)))))

