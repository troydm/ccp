(ns ccp.exp
    (:use [ccp.core])
    (:require [clojure.string :as str]))

(defn exp
  "generate parser based on parser expression e and parser sequence/map pm of parsers"  
  ([e] (exp e {}))
  ([e pm]
    (letfn [(apply-<*> [ps] (if (= (count ps) 1) (first ps) (apply <*> ps)))
            (apply-<*!> [ps] (if (<= (count ps) 1) (first ps) (<$> (apply <*!> ps) #(if (= (count %) 1) (first %) %))))
            (apply-<|> [ps] (if (= (count ps) 1) (first ps) (apply <|> ps)))
            (get-empty-group-index [mp]
              (loop [i 0]
                    (let [si (str i)]
                      (if (contains? mp si)
                        (recur (+ i 1))
                        si
                        ))))
            (fix-parser-name [pn] (cond
                                    (keyword? pn) (name pn)
                                    (symbol? pn) (str pn)
                                    (string? pn) pn
                                    (number? pn) (str pn)))
            (fix-parser-map [pm] (into {} (for [[k v] pm] [(fix-parser-name k) (if (string? v) (exp v) v)])))
            (starts-with? [s c] (= (.charAt s 0) c))
            (ends-with? [s c] (= (.charAt s (- (.length s) 1)) c))
            (quote? [c] (= c \'))
            (parser-started? [c] (= c \<))
            (parser-ended? [c] (= c \>))
            (group-started? [c] (= c \())
            (group-ended? [c] (= c \)))
            (regexp-started? [c] (= c \/))
            (regexp-ended? [c] (= c \/))
            (choice-started? [c] (= c \[))
            (choice-ended? [c] (= c \]))
            (quantifier-started? [c] (or (= c \?) (= c \*) (= c \+) (= c \{)))
            (quantifier-ended? [c] (or (= c \?) (= c \*) (= c \+) (= c \})))
            (not? [c] (= c \!))
            (and? [c] (= c \&))
            (null? [c] (= c \`))
            (colon? [c] (= c \:))
            (comma? [c] (= c \,))
            (tilde? [c] (= c \~))
            (dquote? [c] (= c \"))
            (sharp? [c] (= c \#))
            (at? [c] (= c \@))
            (char-at [b i] (if (and (>= i 0) (< i (.length b))) (.charAt b i) nil))
            (p-at [ps i] (try 
                           (nth ps i) 
                           (catch IndexOutOfBoundsException _ nil)))
            (conj-not-nil [s e] (if (nil? e) s (conj s e)))
            (apply-to-last2 [ps f] (let [e1 (p-at ps (- (count ps) 2))
                                         e2 (p-at ps (- (count ps) 1))]
                                        (apply f (conj-not-nil (conj-not-nil [] e1) e2))))
            (drop-last! [v] (transient (into [] (drop-last (persistent! v)))))
            (drop-last2! [v] (transient (into [] (drop-last (drop-last (persistent! v))))))
            (replace-char [b c] (.setCharAt b (- (.length b) 1) c))
            (replace-last [ps p] 
              (let [li (- (count ps) 1)]
                (assoc! ps li p)))
            (replace-last-with [ps f] 
              (let [li (- (count ps) 1)]
                (assoc! ps li (f (nth ps li)))))
            (append-char [b c] (.append b c))
            (append-or-replace-char [b c pce] (if pce (replace-char b c) (append-char b c)))
            (get-buffer [b] 
              (let [bn (.toString b)]      ;; getting current parser name
                (do (.setLength b 0) bn))) ;; clearing parser name builder
            ;; conj! string parser if s is not empty or nil
            (conj?-string [s] (if (and (not (nil? s)) (> (.length s) 0)) 
                                (if (= (.length s) 1) (chr (.charAt s 0)) (string s))
                                nil))
            (replace-last-with-quantifier [q] 
              (fn [ps] (if (= (count ps) 0)
                (throw (IllegalArgumentException. "quantifier used without expression"))
                (replace-last-with ps #(apply atleast (into [%] (parse-quantifier q)))))))
            (parse-quantifier [q]
              (cond
                (= q "*") (list 0 (Integer/MAX_VALUE))
                (= q "?") (list 0 1)
                (= q "+") (list 1 (Integer/MAX_VALUE))
                (and (starts-with? q \{) (ends-with? q \})) 
                  (let [q (.substring q 1 (- (.length q) 1))]
                    (cond
                      (ends-with? q \,) (list (Integer/parseInt (.substring q 0 (- (.length q) 1))) (Integer/MAX_VALUE))
                      (not (= (.indexOf q ",") -1)) (map #(Integer/parseInt %) (str/split q #"," 2))
                      :else (try 
                              (let [n (Integer/parseInt q)] (list n n))
                              (catch NumberFormatException e 
                                     (throw (IllegalArgumentException. (str q " is not valid quantifier")))))))))
            (apply-ps [acc ps] 
              (if (empty? ps)
                acc
                (loop [acc acc [h & t] ps]
                  (if (empty? t)
                    (h acc)
                    (recur (h acc) t)))))
            (mut-acc [acc ps p]
              (apply-ps (conj! acc p) ps))
            (next [s]
              (let [{i :i acc :acc} s]
                (assoc s :i (+ i 1))))
            (next-p [s p]
              (let [{i :i acc :acc ps :ps} s]
                (assoc s :i (+ i 1) :acc (mut-acc acc ps p) :ps nil)))
            (drop-p [s]
              (let [{i :i acc :acc} s]
                (assoc s :i (+ i 1) :acc (drop-last acc))))
            (next-m [s m]
              (let [{i :i} s]
                (assoc s :i (+ i 1) :m m)))
            (next-pm [s m p]
              (let [{i :i acc :acc ps :ps} s]
                  (assoc s :i (+ i 1) :m m :acc (mut-acc acc ps p) :ps nil)))
            (mut-gc [s ngc] (assoc s :gc ngc))
            (next-fm [s m f]
              (let [{i :i acc :acc} s]
                (assoc s :i (+ i 1) :m m :acc (f acc))))
            (next-psf [s mf]
              (let [{i :i ps :ps} s]
                (assoc s :i (+ i 1) :ps (conj ps mf))))
            (next-ps [s p]
              (next-psf s  (fn [ps] (replace-last-with ps #(p %)))))
            (parse-exp [e mp]
              (let [elen (.length e) b (new StringBuilder)]
                (loop [s {:m 0 :gc 0 :i 0 :acc (transient []) :mp mp :ps nil}]
                      (let [{i :i m :m gc :gc mp :mp} s]
                      (if (>= i elen)
                        (if (= m 0)
                          (persistent! (:acc s))
                          (throw (IllegalArgumentException. (str "invalid parsing expression: " e))))
                        ;; get character and previous character (nil if first position)
                        (let [c (.charAt e i) 
                              pc (char-at e (- i 1)) 
                              ppc (char-at e (- i 2)) 
                              pce (and (= pc \\) (not (= ppc \\)))] 
                          (cond
                            (= m 0)
                            (if pce
                              (do (get-buffer b)
                                (cond
                                  (= c \s)
                                  (recur (next-p s ws))
                                  (= c \S)
                                  (recur (next-p s non-ws))
                                  (= c \d)
                                  (recur (next-p s digit))
                                  (= c \D)
                                  (recur (next-p s non-digit))
                                  (= c \a)
                                  (recur (next-p s alpha))
                                  (= c \A)
                                  (recur (next-p s non-alpha))
                                  (= c \w)
                                  (recur (next-p s letter))
                                  (= c \W)
                                  (recur (next-p s non-letter))
                                  (= c \u)
                                  (recur (next-p s upper))
                                  (= c \U)
                                  (recur (next-p s non-upper))
                                  (= c \l)
                                  (recur (next-p s lower))
                                  (= c \l)
                                  (recur (next-p s non-lower))
                                  :else
                                  (recur (next-p s (chr c)))))
                              (cond 
                                (dquote? c)
                                (recur (next-ps s <str>))
                                (colon? c)
                                (recur (next-ps s <keyword>))
                                (comma? c)
                                (recur (next-psf s drop-last!))
                                (and? c)
                                (recur (next-psf s (fn [ps] 
                                                     (let [e (apply-to-last2 ps <<)]
                                                       (conj! (drop-last2! ps) e)
                                                       ))))
                                (at? c)
                                (recur (next-psf s (fn [ps] 
                                                     (let [e (apply-to-last2 ps >>)]
                                                       (conj! (drop-last2! ps) e)
                                                       ))))
                                (null? c)
                                (recur (next-ps s null))
                                (not? c)
                                (recur (next-ps s no))
                                (sharp? c)
                                (recur (next-ps s flat))
                                (choice-started? c)
                                (recur (next-m s 5))
                                (regexp-started? c)
                                (recur (next-m s 6))
                                (quote? c)
                                (recur (next-m s 4))
                                (group-started? c)
                                (recur (next-m s 3))
                                (parser-started? c)
                                (recur (next-m s 2))
                                (quantifier-started? c)
                                (if (quantifier-ended? c) 
                                  (recur (next-fm s 0 (replace-last-with-quantifier (str c))))
                                  (do (append-char b c) (recur (next-m s 1))))
                                (= c \\)
                                (do (append-char b c) (recur (next s)))
                                (= c \^)
                                (recur (next-p s bol))
                                (= c \$)
                                (recur (next-p s eol))
                                (= c \.)
                                (recur (next-p s any-char))
                                (= c \_)
                                (recur (next-p s (consume 1)))
                                :else 
                                (recur (next-p s (chr c)))))
                            (= m 5)
                            (if (and (choice-ended? c) (not pce))
                              (recur (next-pm s 0 (apply-<|> (parse-exp (get-buffer b) mp)))) ;; adding choice parser
                              (do (append-char b c) 
                                (recur (next s)))) ;; building string
                            (= m 6)
                            (if (and (regexp-ended? c) (not pce))
                              (let [cs (get-buffer b)]
                                (if (or (nil? cs) (empty? cs))
                                  (recur (next-m s 0))
                                  (recur (next-pm s 0 (match cs))))) ;; adding string parser
                              (do (append-or-replace-char b c pce) 
                                (recur (next s)))) ;; building string
                            (= m 4)
                            (if (and (quote? c) (not pce))
                              (let [cs (conj?-string (get-buffer b))]
                                (if (nil? cs)
                                  (recur (next-m s 0))
                                  (recur (next-pm s 0 cs)))) ;; adding string parser
                              (do (append-or-replace-char b c pce) 
                                (recur (next s)))) ;; building string
                            (= m 1)
                            (do (append-char b c)
                              (if (quantifier-ended? c)
                                (let [q (get-buffer b)] ;; getting current quantifier 
                                  (recur (next-fm s 0 (replace-last-with-quantifier q))))
                                (recur (next s))))
                            (= m 2)
                            (if (and (parser-ended? c) (not pce))
                              (let [pn (get-buffer b)] ;; getting current parser name
                                (if (not (contains? mp pn)) 
                                  (throw (IllegalArgumentException. (str "couldn't find parser " pn " in parser map: " mp)))
                                  (recur (next-pm s 0 (mp pn)))))
                              (do (append-or-replace-char b c pce)
                                (recur (next s))))
                            (= m 3)
                            (if (and (group-ended? c) (not pce) (= gc 0))
                              (let [e (get-buffer b)] ;; getting group expression
                                (let [pe (exp e pm)]
                                  (recur (next-pm (assoc s :mp (assoc mp (get-empty-group-index mp) pe)) 0 pe))))
                              (do (append-char b c)
                                (recur (next 
                                         (cond 
                                           (group-started? c) 
                                           (mut-gc s (+ gc 1))
                                           (group-ended? c) 
                                           (mut-gc s (- gc 1))
                                           :else s)
                                         ))))
                            )))))))
            ]
      (let [mp (fix-parser-map (if (not (map? pm)) (zipmap (range) pm) pm))]
        (if (tilde? (.charAt e 0))
          (apply-<*!> (parse-exp (subs e 1) mp))
          (apply-<*> (parse-exp e mp)))))))

