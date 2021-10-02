{-# LANGUAGE CPP #-}
module Hoff.Vector where

import qualified Data.HashMap.Internal as HI
import qualified Data.HashMap.Strict as HM
import qualified Data.HashTable.ST.Cuckoo as HT
import           Data.STRef
import qualified Data.Vector as V
import           Data.Vector.Algorithms
import           Data.Vector.Algorithms.Intro
import qualified Data.Vector.Algorithms.Radix as R
import qualified Data.Vector.Fusion.Bundle as Bundle
import qualified Data.Vector.Generic as G hiding (toList)
import qualified Data.Vector.Mutable as M
import           Yahp hiding (sortBy)

instance (Hashable a) => Hashable (V.Vector a) where
  hashWithSalt salt = hashWithSalt salt . toList
  {-# INLINE hashWithSalt #-}

unsafeBackpermuteMaybe2 :: (G.Vector v a, G.Vector v a, G.Vector v (Maybe Int) , G.Vector v Int)
  => v a -> v a -> v Int -> v (Maybe Int) -> v a
{-# INLINE unsafeBackpermuteMaybe2 #-}
unsafeBackpermuteMaybe2 v1 v2 is1 is2 = seq v1 $ seq v2
                       $ G.unstream
                       $ Bundle.unbox
                       $ Bundle.zipWith index (G.stream is1) (G.stream is2)
  where
    {-# INLINE index #-}
    index left right = maybe (G.basicUnsafeIndexM v1 left) (G.basicUnsafeIndexM v2) right

unsafeBackpermuteMaybeV :: (G.Vector v a, G.Vector v (Maybe a), G.Vector v (Maybe Int)) => v a -> v (Maybe Int) -> v (Maybe a)
{-# INLINE unsafeBackpermuteMaybeV #-}
unsafeBackpermuteMaybeV v is =  seq v
                                $ G.unstream
                                $ Bundle.unbox
                                $ Bundle.map index
                                $ G.stream is
  where
    {-# INLINE index #-}
    index = traverse $ G.basicUnsafeIndexM v

-- | conditional a b c = "if a then b else c"
conditional :: (G.Vector v a, G.Vector v Bool) => v Bool -> v a -> v a -> v a
conditional = G.zipWith3 $ \a b c -> if a then b else c
{-# INLINABLE conditional #-}


-- | returns the indices of the unique elements
distinctVIndicesSuperSlow :: Ord a => V.Vector a -> V.Vector Int
distinctVIndicesSuperSlow = fmap fst . nubBy (comparing snd) . V.indexed 
{-# INLINABLE distinctVIndicesSuperSlow #-}


-- | returns the indices of the unique elements
distinctVIndicesHm :: (Eq a, Hashable a) => V.Vector a -> V.Vector Int
distinctVIndicesHm = sortV compare  . V.fromList . HM.elems . HM.fromList . toList . fmap swap . V.reverse . V.indexed
{-# INLINABLE distinctVIndicesHm #-}

distinctVIndicesHt :: (Eq a, Hashable a) => V.Vector a -> V.Vector Int
distinctVIndicesHt v = V.create $ do
  m <- M.unsafeNew $ length v
  ht <- HT.newSized $ length v
  let handle mIdx vIdx val = do exists <- HT.lookup ht val
                                case exists of
                                  Just () -> pure mIdx
                                  _       -> do
                                    M.write m mIdx vIdx
                                    succ mIdx <$ HT.insert ht val ()
                              
  mIdx <- V.ifoldM'  handle 0 v
  pure $ M.take mIdx m
{-# INLINABLE distinctVIndicesHt #-}
  
  
distinctVIndices :: (Eq a, Hashable a) => V.Vector a -> V.Vector Int
distinctVIndices v = V.create $ do 
  m <- M.unsafeNew $ length v
  hm <- newSTRef $ HM.empty
  let handle mIdx vIdx val = do exists <- HM.lookup val <$> readSTRef hm
                                case exists of
                                  Just () -> pure mIdx
                                  _       -> do
                                    M.write m mIdx vIdx
                                    succ mIdx <$ modifySTRef' hm (HI.unsafeInsert val ())
                              
  mIdx <- V.ifoldM'  handle 0 v
  pure $ M.take mIdx m
{-# INLINABLE distinctVIndices #-}
  
distinctV :: (Eq a, Hashable a) => V.Vector a -> V.Vector a
distinctV v = V.create $ do 
  m <- M.unsafeNew $ length v
  hm <- newSTRef $ HM.empty
  let handle mIdx val = do exists <- HM.lookup val <$> readSTRef hm
                           case exists of
                             Just () -> pure mIdx
                             _       -> do
                               M.write m mIdx val
                               succ mIdx <$ modifySTRef' hm (HI.unsafeInsert val ())
                              
  mIdx <- V.foldM'  handle 0 v
  pure $ M.take mIdx m
{-# INLINABLE distinctV #-}
  
  
sortVr :: R.Radix x => V.Vector x -> V.Vector x
sortVr v = V.create $ bind (V.thaw v) $ \m -> m <$ R.sort m
{-# INLINABLE sortVr #-}

sortV :: (Comparison x) -> V.Vector x -> V.Vector x
sortV cmp v = V.create $ bind (V.thaw v) $ \m -> m <$ sortBy cmp m
{-# INLINABLE sortV #-}


