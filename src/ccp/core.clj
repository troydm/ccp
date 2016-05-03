(ns ccp.core
  (:require [clojure.string :as str]))

(defprotocol ParseStreamFactory
  "A parse stream factory protocol"
  (parse-stream [d]))

(defprotocol ParseStreamCharSequenceFactory
  "A parse stream char sequence factory protocol"
  (char-sequence [ps]))

(defprotocol ParseStream
  "A parse stream protocol used by parser"
  (reset [ps])
  (current-elem [ps])
  (prev-elem [ps])
  (next-elem [ps])
  (get-state [ps])
  (restore-state [ps st])
  (state-changed? [ps st])
  (start-state? [ps])
  (end-state? [ps])
  (get-user-state [ps])
  (set-user-state [ps nu])
  (describe-error [ps pe]))

(deftype ParserError [ps state msg]
  Object
  (toString [self] (describe-error ps self)))

(defn parser-error [ps msg] (ParserError. ps (get-state ps) msg))
(defn parser-error-stream [pe] (.ps pe))
(defn parser-error-state [pe] (.state pe))
(defn parser-error-message [pe] (.msg pe))
(defn parser-error? [pe] (= (type pe) ParserError))

(defmacro parsed [e p t f]
  `(let [~e ~p]
     (if (parser-error? ~e) ~f ~t)))

(defn- line-col
  "find out line/column at position p in String s"
  [s p]
  (let [len (.length s)]
    (loop [i 0 l 1 c 1]
          (if (>= i p)
            `(~l ~c)
        (if (= i len)
          `(~l ~(+ c (- p i)))
          (cond 
            (= (.charAt s i) \newline)
            (if (= p i) 
              `(~l ~(inc c)) 
              (recur (inc i) (inc l) 1))
            (= (.charAt s i) \return)
            (if (= p i) 
              `(~l ~(inc c)) 
              (if (and (< (inc i) len) (= (.charAt s (inc i)) \newline))
                (if (= p (inc i)) `(~l ~c) (recur (inc (inc i)) (inc l) 1))
                (recur (inc i) (inc l) 1)))
            :else
            (recur (inc i) l (inc c))
            ))))))
    

(deftype StringParseStreamCharSequence [ps p l]
  CharSequence
  (charAt [_ i] 
    (let [i (+ p i)]
      (if (or (< i 0) (>= i (.length (.s ps))))
        (throw (IndexOutOfBoundsException.))
        (.charAt (.s ps) i))))
  (length [_] l)
  (subSequence [_ s e] (StringParseStreamCharSequence. ps (+ p s) (- e s)))
  (toString [_] (.substring (.s ps) p (+ p l))))

(deftype StringParseStream [^:unsynchronized-mutable p s ^:unsynchronized-mutable u]
  ParseStream
  (reset [ps] (set! p -1))
  (current-elem [ps] (if (or (start-state? ps) (end-state? ps)) nil (.charAt s p)))
  (prev-elem [ps] (if (not (start-state? ps))
                    (set! p (- p 1)))
                  (current-elem ps))
  (next-elem [ps] (if (not (end-state? ps))
                    (set! p (+ p 1)))
                  (current-elem ps))
  (get-state [ps] p)
  (restore-state [ps st] (if (and (>= st -1) (<= st (.length s))) (set! p st)) p)
  (state-changed? [ps st] (not= st p))
  (start-state? [ps] (= -1 p))
  (end-state? [ps] (= p (.length s)))
  (get-user-state [ps] u)
  (set-user-state [ps nu] (set! u nu))
  (describe-error [ps pe]
    (let [[l c] (line-col s (parser-error-state pe))]
    (str "expecting " (parser-error-message pe) " on line " l " col " c)))
  ParseStreamCharSequenceFactory
  (char-sequence 
    [ps] 
    (StringParseStreamCharSequence. ps (get-state ps) (- (.length (.s ps)) (get-state ps)))))

(deftype SequenceParseStream [^:unsynchronized-mutable p s ^:unsynchronized-mutable u]
  ParseStream
  (reset [ps] (set! p -1))
  (current-elem [ps] (if (or (start-state? ps) (end-state? ps)) nil (nth s p)))
  (prev-elem [ps] (if (not (start-state? ps))
                    (set! p (- p 1)))
                  (current-elem ps))
  (next-elem [ps] (if (not (end-state? ps))
                    (set! p (+ p 1)))
                  (current-elem ps))
  (get-state [ps] p)
  (restore-state [ps st] (if (and (>= st -1) (<= st (count s))) (set! p st)) p)
  (state-changed? [ps st] (not= st p))
  (start-state? [ps] (= -1 p))
  (end-state? [ps] (= p (count s)))
  (get-user-state [ps] u)
  (set-user-state [ps nu] (set! u nu))
  (describe-error [ps pe]
    (let [p (parser-error-state pe)]
    (str "expecting " (parser-error-message pe) " on index " p " in sequence, but got: " (nth s p)))))

(extend-type String
    ParseStreamFactory
    (parse-stream [d] (StringParseStream. -1 d nil)))

(extend-type java.util.List
    ParseStreamFactory
    (parse-stream [d] (SequenceParseStream. -1 d nil)))

(extend-type clojure.lang.PersistentVector
    ParseStreamFactory
    (parse-stream [d] (SequenceParseStream. -1 d nil)))

(extend-type clojure.lang.PersistentVector$TransientVector
    ParseStreamFactory
    (parse-stream [d] (SequenceParseStream. -1 d nil)))

(extend-type clojure.lang.PersistentList
    ParseStreamFactory
    (parse-stream [d] (SequenceParseStream. -1 d nil)))

(extend-type clojure.lang.PersistentList$EmptyList
    ParseStreamFactory
    (parse-stream [d] (SequenceParseStream. -1 d nil)))

(extend-type clojure.lang.PersistentHashSet
    ParseStreamFactory
    (parse-stream [d] (SequenceParseStream. -1 d nil)))

(extend-type clojure.lang.PersistentArrayMap
    ParseStreamFactory
    (parse-stream [d] (SequenceParseStream. -1 (seq d) nil)))

;; this empty stream is used by few parsers
(def empty-stream (parse-stream ""))

;; used by multiple parsers
(defn- n-args 
  "return number of arguments function can take"
  [f]
  (-> f class .getDeclaredMethods first .getParameterTypes alength))

(defn rewind
  "try parsing p and restore stream state on failure"
  [p]
  (fn [ps] (let [st (get-state ps) e (p ps)]
             (if (parser-error? e)
               (do (restore-state ps st) e)
               e))))

(defn restore
  "try parsing p and restore stream state afterwards"
  [p]
  (fn [ps] (let [st (get-state ps) e (p ps)]
             (do (restore-state ps st) e))))

(defn return
  "try parsing p and on success return r otherwise return parser error"
  [p r]
  (fn [ps] (parsed e (p ps) r e)))

(defn null
  "try parsing p and on success return nil"
  [p]
  (return p nil))

(def user-state
  "get current parse stream user state"
  (fn [ps] (get-user-state ps)))

(defn set-user-state!
  "set current parse stream user state"
  [s]
  (fn [ps] (set-user-state ps s)))

(defn modify-user-state!
  "apply function to current parse stream user state"
  [f]
  (fn [ps] (set-user-state ps (f (get-user-state ps)))))

(defmacro defer
  "if p is not defined yet, return a lazy parser instead, used for forward declarations"
  [p]
  `(if (clojure.test/function? ~p)
    ~p
    (fn [ps#] (~p ps#))))

(defn satisfy
  "satisfy predicate pd, if not return failure with m"
  [pd m]
  (if (= (n-args pd) 2)
    (fn [ps] (let [e (next-elem ps)]
               (if (pd e ps)
                 e
                 (parser-error ps m))))
    (fn [ps] (let [e (next-elem ps)]
               (if (pd e)
                 e
                 (parser-error ps m))))))

(defn satisfy-char
  "satisfy predicate pd for character, if not return failure with m" 
  [pd m]
  (satisfy #(and (char? %) (pd %)) m))

(defn chr
  "expect character c"
  [c]
  (rewind (satisfy-char #(= % c) (str (pr-str c) " character"))))

(def digit
  "expect digit character"
  (rewind (satisfy-char #(Character/isDigit %) "digit")))

(def non-digit
  "expect non-digit character"
  (rewind (satisfy-char #(not (Character/isDigit %)) "non-digit")))

(def ws
  "expect whitespace character"
  (rewind (satisfy-char #(Character/isWhitespace %) "whitespace")))

(def non-ws
  "expect non-whitespace character"
  (rewind (satisfy-char #(not (Character/isWhitespace %)) "non-whitespace")))

(def alpha
  "expect alphanum character"
  (rewind (satisfy-char #(or (Character/isLetter %) (Character/isDigit %)) "alphanum")))

(def non-alpha
  "expect non-alphanum character"
  (rewind (satisfy-char #(not (or (Character/isLetter %) (Character/isDigit %))) "non-alphanum")))

(def letter
  "expect letter character"
  (rewind (satisfy-char #(Character/isLetter %) "letter")))

(def non-letter
  "expect non-letter character"
  (rewind (satisfy-char #(not (Character/isLetter %)) "non-letter")))

(def upper
  "expect uppercase character"
  (rewind (satisfy-char #(Character/isUpperCase %) "uppercase")))

(def non-upper
  "expect non-uppercase character"
  (rewind (satisfy-char #(not (Character/isUpperCase %)) "non-uppercase")))

(def lower
  "expect lowercase character"
  (rewind (satisfy-char #(Character/isLowerCase %) "lowercase")))

(def non-lower
  "expect non-lowercase character"
  (rewind (satisfy-char #(not (Character/isLowerCase %)) "non-lowercase")))

(def any-char
  "expect any character"
  (rewind (satisfy char? "any character")))

(def bol
  "expect beginning of line, returns nil"
  (fn [ps]
      (if (start-state? ps) 
        nil
        (let [c (current-elem ps)]
          (if (or (= c \newline) (= c \return))
            nil
            (parser-error ps "beginning of line"))))))
  
(def eol
  "expect end of line, supports windows, unix and mac newline types, returns \newline"
  (rewind 
    (fn [ps]
        (let [e (next-elem ps)]
          (if (or (end-state? ps) (= e \newline))
            \newline
            (if (= e \return)
              (let [st (get-state ps) e (next-elem ps)]
                (if (= e \newline) e (do (restore-state ps st) \newline)))
              (parser-error ps "end of line character")))))))

(def neol
  "expect end of line, returns nil on success"
  (null eol))

(def bos
  "expect beginning of stream"
  (fn [ps] (if (start-state? ps) nil (parser-error ps "beginning of stream"))))

(def eos
  "expect end of stream"
  (fn [ps] (if (end-state? ps) nil (parser-error ps "end of stream"))))

(def any-elem
  "expect any element"
  (fn [ps] (next-elem ps)))

(defn elem
  "expect element e named with name, name is used for parse error"
  [e & {:keys [name]}]
  (rewind (satisfy #(= e %) (if (nil? name) (str e) name))))

(defn match
  "match regex re, on success returns result of the match"
  [re]
  (let [re (if (string? re) (re-pattern re) re)]
    (rewind (fn [ps] 
              (next-elem ps) 
              (let [d (re-matches re (char-sequence ps))]
                (if d 
                  (loop [i (- (.length (if (vector? d) (first d) d)) 1)]
                    (if (> i 0) (do (next-elem ps) (recur (- i 1))) d))
                  (parser-error ps (str re))))))))

(defn collect
  "collect exactly n elems and return vector containing those elements"
  [n]
  (cond 
    (< n 0) 
    (throw (IllegalArgumentException. "n can't be negative"))
    (= n 0) 
    (fn [ps] [])
    :else
    (rewind (fn [ps]
              (loop [e (next-elem ps) n n acc (transient [])]
                (if (nil? e)
                  (parser-error ps (str n " element" (if (> n 1) "s" nil)))
                  (if (= n 1) 
                    (persistent! (conj! acc e))
                    (recur (next-elem ps) (- n 1) (conj! acc e)))))))))

(defn consume
  "consume exactly n elems and return nil"
  [n]
  (cond 
    (< n 0)
    (throw (IllegalArgumentException. "n can't be negative"))
    (= n 0)
    (fn [ps] nil)
    :else
    (rewind (fn [ps]
              (loop [e (next-elem ps) n n]
                (if (nil? e) 
                  (parser-error ps (str n " element" (if (> n 1) "s" nil)))
                  (if (= n 1) 
                    nil
                    (recur (next-elem ps) (- n 1)))))))))

(defn string
  "parse string s"
  [s]
  (if (empty? s) (throw (IllegalArgumentException. "parsing string can't be empty"))
    (rewind (fn [ps]
             (loop [e (next-elem ps) p 0]
                   (if (= (.charAt s p) e)
                     (if (= (+ p 1) (.length s))
                       s
                       (recur (next-elem ps) (+ p 1)))
                     (parser-error ps (str "'" (.charAt s p) "' from '" s "' string"))))))))

(defn >>
  "execute parsers sequentially and return result of the last one"
  [& p]
  (fn [ps]
      (loop [[h & t] p]
            (parsed e (h ps) 
                    (if (empty? t) e (recur t)) 
                    e))))

(defn >>!
  "execute parsers sequentially and return result of the last non-nil parse result"
  [& p]
  (fn [ps]
      (loop [[h & t] p last-elem nil]
            (parsed e (h ps) 
                    (if (empty? t) 
                      (if (nil? e) last-elem e) 
                      (recur t (if (nil? e) last-elem e))) 
                    e))))

(defn >>=
  "execute parser and return result of applying f to parsed result and optionally a parse stream"
  [p f]
  (if (= (n-args f) 2)
    (fn [ps]
        (parsed e (p ps) (f e ps) e))
    (fn [ps]
        (parsed e (p ps) (f e) e))))

(defn <<
  "execute parsers sequentially and return result of the first one"
  [& p]
  (fn [ps]
      (parsed first-elem ((first p) ps) 
              (loop [[h & t] (rest p)]
                    (if (nil? h)
                      first-elem
                      (parsed e (h ps) (recur t) e)))
              first-elem)))

(defn <<!
  "execute parsers sequentially and return result of the first non-nil parse result"
  [& p]
  (fn [ps]
      (parsed first-elem ((first p) ps) 
              (loop [first-elem first-elem [h & t] (rest p)]
                    (if (nil? h)
                      first-elem
                      (parsed e (h ps) (recur (if (nil? first-elem) e first-elem) t) e)))
              first-elem)))

(defmacro letp
  "binds parser results to variables sequentially, same as let but uses parsers as values"
  [[& parsers] & body]
  (if (or (zero? (count parsers)) (not (zero? (mod (count parsers) 2))))
    (throw (IllegalArgumentException. "invalid number of parsers"))
    (let [[e p & r] parsers]
      (if (nil? r)
        `(>>= ~p (fn [~e ps#] ~@body))
        `(>>= ~p (fn [~e ps#] ((letp ~r ~@body) ps#)))))))

(defn <$>
  "try parsing p and on success apply f to result of p otherwise return p's parser-error"
  [p f]
  (fn [ps] (parsed e (p ps) (f e) e)))

(defn <*>
  "execute parsers sequentially and return vector of results, fails if one of them fails"
  [& p]
  (if (empty? p)
    (throw (IllegalArgumentException. "no parsers specified"))
    (fn [ps]
      (loop [[h & t] p acc (transient [])]
        (parsed e (h ps)
                (if (empty? t)
                  (persistent! (conj! acc e))
                  (recur t (conj! acc e)))    
                e)))))

(defn <*!>
  "execute parsers sequentially and return vector of non-nil results, fails if one of them fails"
  [& p]
  (if (empty? p)
    (throw (IllegalArgumentException. "no parsers specified"))
    (fn [ps]
      (loop [[h & t] p acc (transient [])]
        (let [e (h ps)]
          (if (parser-error? e)
            e
            (let [acc (if (nil? e) acc (conj! acc e))]
              (if (empty? t)
                (persistent! acc)
                (recur t acc)))))))))

(defn <str>
  "parse p and apply str to it's result"
  [p]
  (fn [ps]
      (parsed e (p ps) (apply str e) e)))

(defn <str*>
  "execute parsers sequentially and return string of results, fails if one of them fails"
  [& p]
  (<str> (apply <*> p)))

(defn <keyword>
  "parse p and apply keyword to it's result"
  [p]
  (fn [ps]
    (parsed e (p ps) 
            (keyword 
              (cond 
                (string? e)
                e
                (coll? e)
                (apply str e)
                :else (str e)))
            e)))
  
(defn <keyword*>
  "execute parsers sequentially and return keyword of results, fails if one of them fails"
  [& p]
  (<keyword> (apply <str*> p)))

(defn <|>
  "execute parsers sequentially and return first successful result, fails if one of them fails while consuming input or all of them fail"
  [& p]
  (if (empty? p)
    (throw (IllegalArgumentException. "no parsers specified"))
    (fn [ps]
      (loop [[h & t] p ms (transient [])]
        (let [st (get-state ps) e (h ps)]
          (if (parser-error? e)
            (if (state-changed? ps st)
              e
              (if (empty? t) 
                (let [ms (persistent! (conj! ms (parser-error-message e)))
                      st (get-state ps)
                      _ (next-elem ps)
                      pe (parser-error ps (str/join " or " ms))
                      _ (restore-state ps st)]
                  pe)
                (recur t (conj! ms (parser-error-message e)))))
            e))))))

(defn <?>
  "try p, if it fails change failure msg with m"
  [p m]
  (fn [ps]
      (let [e (p ps)]
        (if (parser-error? e) (parser-error ps m) e))))

(defn fail
  "fail p, parser p must not accept nil otherwise failure message won't be correct"
  [p]
  (restore (cond
             (= p bos)
             (fn [ps] (next-elem ps) (parser-error ps "beginning of stream"))
             (= p eos)
             (fn [ps] (next-elem ps) (parser-error ps "end of stream"))
             (= p any-elem)
             (fn [ps] (next-elem ps) (parser-error ps "any element"))
             :else
             (fn [ps]
                 (next-elem ps)
                 (parsed e (p empty-stream)
                         (parser-error ps (str e)) 
                         (parser-error ps (parser-error-message e)))))))

(defn fail-message
  "makes p fail and returns failure message, note: not a parser function"
  [p]
  (parser-error-message ((fail p) empty-stream)))

(defn no
  "if p fails succeds with nil, otherwise fails"
  [p]
  (fn [ps]
      (parsed e (p ps) (parser-error ps (str "not " (fail-message p))) nil)))

(defn option
  "try p, if it fails without consuming any input return r, otherwise return value returned by p"
  [p r]
  (fn [ps] (let [st (get-state ps) e (p ps)]
             (if (parser-error? e)
               (if (state-changed? ps st) e r)
               e))))

(defn op
  "return operator parser p with precedence o"
  [p o]
  (fn [ps]
      (parsed e (p ps) `(~o ~e) e)))

(defn op-expr
  "parse parser p expression and then optionally parse any of operator parser from ops sequence followed by another p expression and return expressions ordered by operator precedence with f applied to each expression"
  ([p ops] (op-expr p ops identity))
  ([p ops f]
  (let [ops (apply <|> ops)]
    (letfn [(op>= [o1 o2] (>= (first o1) (first o2)))
            (exps [ps] 
              (parsed e (p ps) 
                      (loop [acc (transient [e])]
                            (parsed o (ops ps)
                                    (parsed e (p ps)
                                            (recur (conj! (conj! acc o) e))
                                            e)
                                    (persistent! acc)))
                      e))
            (reduce-exps [es]
              (loop [es es acc []]
                    (cond 
                      (>= (count es) 5)
                      (let [[a o1 b o2 c & d] es]
                        (if (op>= o1 o2)
                          (recur (into [(f `(~(second o1) ~a ~b)) o2 c] d) acc)
                          (recur (into [b o2 c] d) (into acc [a o1]))))
                      (= (count es) 3)
                      (let [[a o b] es] (into acc [(f `(~(second o) ~a ~b))]))
                      :else (into acc es))))
            (reduce-while [es]
              (loop [es (reduce-exps es)]
                    (if (> (count es) 1)
                      (recur (reduce-exps es))
                      es)))
            ]
      (fn [ps]
        (parsed es (exps ps) (first (reduce-while es)) es))))))

(defn many
  "try p, 0 or more times and return result"
  [p]
  (fn [ps]
      (loop [e (p ps) acc (transient [])]
            (if (parser-error? e)
              (persistent! acc)
              (recur (p ps) (conj! acc e))))))

(defn many1
  "try p, 1 or more times and return result"
  [p]
  (fn [ps]
      (let [e (p ps)]
        (if (parser-error? e)
          e
          (loop [acc (transient [e]) e (p ps)]
                (if (parser-error? e)
                  (persistent! acc)
                  (recur (conj! acc e) (p ps))))))))
      
(defn many-till
  "try p, 0 or more times until end succeeds"
  [p end]
  (fn [ps]
      (loop [e (end ps) acc (transient [])]
            (if (parser-error? e)
              (let [e2 (p ps)]
                (if (parser-error? e2)
                  e
                  (recur (end ps) (conj! acc e2))))
                (persistent! acc)))))

(defn many1-till
  "try p, 1 or more times until end succeeds"
  [p end]
  (fn [ps]
      (let [e (p ps)]
        (if (parser-error? e)
          e
          (loop [acc (transient [e]) e (end ps)]
                (if (parser-error? e)
                  (let [e2 (p ps)]
                    (if (parser-error? e2)
                      e
                      (recur (conj! acc e2) (end ps))))
                  (persistent! acc)))))))

(defn atleast
  "try p atleast n times but no more than m"
  [p n m]
  (cond 
    (and (= n 0) (= m 1)) 
    (option p nil)
    (and (= n 1) (= m 1)) 
    p
    (and (= n 0) (= m Integer/MAX_VALUE)) 
    (many p)
    (and (= n 1) (= m Integer/MAX_VALUE)) 
    (many1 p)
    :else
    (fn [ps]
        (loop [i 0 e (p ps) acc (transient [])]
              (if (parser-error? e)
                (if (>= i n) (persistent! acc) e)
                (let [i (+ i 1)]
                  (if (>= i m) 
                    (persistent! (conj! acc e))
                    (recur i (p ps) (conj! acc e)))))))))

(defn flat
  "parse p and flatten result"
  [p]
  (fn [ps]
    (parsed e (p ps)
            (if (coll? e)
              (cond
                (list? e)
                (flatten e)
                (vector? e)
                (into [] (flatten e)))
              e)
            e)))

(defn between
  "parse p between b and e and return p's result"
  [b p e]
  (fn [ps]
      (parsed b (b ps) 
              (parsed p (p ps)
                      (parsed e (e ps) p e)
                      p)
              b)))

(defn between?
  "parse p optionally between b and e and return p's result"
  [b p e]
  (fn [ps]
      (parsed b (b ps) 
              (parsed p (p ps)
                      (parsed e (e ps) p e)
                      p)
              (parsed p (p ps) p p))))

(defn separated
  "parse p's seperated by s"
  [p s]
  (fn [ps]
      (loop [e (p ps) acc (transient [])]
            (if (parser-error? e) 
              e
              (parsed _ (s ps)
                      (recur (p ps) (conj! acc e))
                      (persistent! (conj! acc e)))))))

(defn separated!
  "parse p's seperated by s, allows ending s without following p"
  [p s]
  (fn [ps]
      (parsed e (p ps)
              (loop [acc (transient [e])]
                    (parsed _ (s ps)
                            (parsed e (p ps) (recur (conj! acc e)) (persistent! acc))
                            (persistent! acc)))
      e)))

(defn parse
  "parse d data using p and return result, resets parse stream"
  [p d]
  (p (if (satisfies? ParseStream d)
       (do (reset d) d)
       (parse-stream d))))

(defn parse>
  "parse d data using p and return result"
  [p d]
  (p (if (satisfies? ParseStream d) d (parse-stream d))))

