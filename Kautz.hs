module Kautz (toKautz, fromKautz, kautzOut, kautzIn) where

toKautz :: Int -> Int -> Int -> [Int]
toKautz m n i = f [] n i where
  f [] r k = f [k `mod` (m + 1)] (r - 1) (k `div` (m + 1))
  f acc@(h:_) 0 k = (if k >= h then k + 1 else k):acc
  f acc@(h:_) r k = f (x:acc) (r - 1) (k `div` m) where
    y = k `mod` m
    x = if y >= h then y + 1 else y

fromKautz :: Int -> [Int] -> Int
fromKautz m as = f 0 as where
  f i (x:y:rest) = f (i * m + (if x > y then x - 1 else x)) (y:rest)
  f i [y] = i * (m + 1) + y
  f _ _   = undefined

kautzOut :: Int -> Int -> Int -> [Int]
kautzOut m n i = fromKautz m . (t ++) . pure <$> filter (/= last t) [0..m]
  where t = tail $ toKautz m n i

kautzIn :: Int -> Int -> Int -> [Int]
kautzIn m n i = fromKautz m . (:s) <$> filter (/= head s) [0..m]
  where s = init $ toKautz m n i

