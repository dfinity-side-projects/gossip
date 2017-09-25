module Kautz (toKautz, fromKautz, kautzOut, kautzIn, kautzSize,
  kautzRecommend, kautzOutRoomy, kautzInRoomy) where

import Data.List
import qualified Data.Map.Strict as M
import Data.Maybe

-- | Returns the size of a Kautz graph with given M and N i.e. (M + 1)*M^N.
kautzSize :: Int -> Int -> Int
kautzSize m n = (m+1)*m^n

-- | Returns node label given M, N, and node index. Nodes are 0-indexed.
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

kautzParams :: M.Map Int (Int, Int)
kautzParams = M.fromList
  [ (20, (4, 1))
  , (30, (5, 1))
  , (42, (6, 1))
  , (80, (4, 2))
  , (108, (3, 3))
  , (150, (5, 2))
  , (252, (6, 2))
  , (320, (4, 3))
  , (392, (7, 2))
  , (750, (5, 3))
  , (972, (3, 5))
  -- , (1100, (10, 2))
  , (1280, (4, 4))
  , (1512, (6, 3))
  , (2744, (7, 3))
  , (4608, (8, 3))
  , (7290, (9, 3))
  , (11000, (10, 3))
  ]

-- | Recommend Kautz M and N parameters for a given number of nodes.
-- Since only numbers of a certain form are available, the Kautz graph
-- contains up to twice the number of nodes.
kautzRecommend :: Int -> (Int, Int)
kautzRecommend sz = snd $ fromJust $ M.lookupGE sz kautzParams

-- | Returns node indices of the direct successors given M, N, a node
-- index, and the number of nodes.
-- The number of nodes may be smaller than the number of nodes in the Kautz
-- graph, so some may have to act as other nodes.
kautzOutRoomy :: Int -> Int -> Int -> Int -> [Int]
kautzOutRoomy m n i lim = filter (/= i) $ nub $ (`mod` lim) <$>
  concatMap (kautzOut m n) [i, i + lim..sz] where sz = kautzSize m n

-- | Returns node indices of the direct predecessors given M, N, a node
-- index, and the number of nodes.
-- The number of nodes may be smaller than the number of nodes in the Kautz
-- graph, so some may have to act as other nodes.
kautzInRoomy :: Int -> Int -> Int -> Int -> [Int]
kautzInRoomy m n i lim = filter (/= i) $ nub $ (`mod` lim) <$>
  concatMap (kautzIn m n) [i, i + lim..sz] where sz = kautzSize m n
