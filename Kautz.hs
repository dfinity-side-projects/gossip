module Kautz (toKautz, fromKautz, kautzOut, kautzIn, kautzSize) where

-- | Returns the size of a Kautz graph with given M and N i.e. (M + 1)*M^N.
kautzSize :: Int -> Int -> Int
kautzSize m n = (m+1)*m^n

-- | Returns node label given M, N, and node index.
toKautz :: Int -> Int -> Int -> [Int]
toKautz m n i = f [] n i where
  f [] r k = f [k `mod` (m + 1)] (r - 1) (k `div` (m + 1))
  f acc@(h:_) 0 k = (if k >= h then k + 1 else k):acc
  f acc@(h:_) r k = f (x:acc) (r - 1) (k `div` m) where
    y = k `mod` m
    x = if y >= h then y + 1 else y

-- | Returns node index given M and node label.
-- The length of the label determines N implicitly.
fromKautz :: Int -> [Int] -> Int
fromKautz m as = f 0 as where
  f i (x:y:rest) = f (i * m + (if x > y then x - 1 else x)) (y:rest)
  f i [y] = i * (m + 1) + y
  f _ _   = undefined

-- | Returns node indices of the direct successors given M, N, and a node
-- index.
kautzOut :: Int -> Int -> Int -> [Int]
kautzOut m n i = fromKautz m . (t ++) . pure <$> filter (/= last t) [0..m]
  where t = tail $ toKautz m n i

-- | Returns node indices of the direct predecessors given M, N, and a node
-- index.
kautzIn :: Int -> Int -> Int -> [Int]
kautzIn m n i = fromKautz m . (:s) <$> filter (/= head s) [0..m]
  where s = init $ toKautz m n i
