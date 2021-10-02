{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE UndecidableInstances #-}

module Hoff.Iterable
  (module Hoff.Iterable
  ,module Reexport
  ) where

import           Data.Coerce
import qualified Data.HashMap.Strict as HM
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Data.Vector.Algorithms.Intro as Intro
import           Data.Vector.Algorithms.Intro as Reexport (Comparison)
import qualified Data.Vector.Generic as G
import qualified Data.Vector.Mutable as M
import           Hoff.H
import           Hoff.Vector
import qualified Prelude as Unsafe
import           Yahp hiding (group)


newtype GroupIndex = GroupIndex { fromGroupIndex :: Int }
  deriving (Show, Num, Eq, Hashable, Ord)

newtype IterIndex = IterIndex { fromIterIndex :: Int }
  deriving (Show, Num, Eq, Hashable, Ord)

type Grouping = Vector (Vector IterIndex)

type KeyVector v = Vector (KeyItem v) 

class Iterable v where
  null                  :: HasCallStack => v            -> Bool
  default null          :: (G.Vector u Int, G.Vector u i, v ~ u i) => v            -> Bool
  null = G.null
  {-# INLINE null #-}

  count                 :: HasCallStack => v            -> Int
  default count         :: (G.Vector u Int, G.Vector u i, v ~ u i) => v            -> Int
  count = G.length
  {-# INLINE count #-}

  -- | negavite n means taking the last n
  take :: Int -> v -> v
  default take          :: (G.Vector u Int, G.Vector u i, v ~ u i) => Int -> v            -> v
  take n | n >= 0 = G.take n
         | True   = \v -> G.drop (G.length v + n) v
  {-# INLINABLE take #-}

  drop :: Int -> v -> v
  default drop          :: (G.Vector u Int, G.Vector u i, v ~ u i) => Int -> v            -> v
  drop = G.drop
  {-# INLINE drop #-}

  unsafeBackpermute     :: Vector IterIndex -> v -> v
  default unsafeBackpermute :: (G.Vector u Int, G.Vector u i, v ~ u i) => Vector IterIndex -> v -> v
  unsafeBackpermute k v = G.unsafeBackpermute v (V.convert (coerce k :: V.Vector Int))
  {-# INLINABLE unsafeBackpermute #-}

  
class Iterable v => DistinctIterable v where
  distinct              :: HasCallStack => v -> v
  default distinct      :: (HasCallStack, Eq a, Hashable a, V.Vector a ~ v) => v -> v
  distinct = distinctV
  {-# INLINE distinct #-}


class (Iterable v) => DictComponent v where
  type KeyItem v :: Type
  type Nullable v :: Type

  -- | get keys from second arg that are directly comparable with the first argS keys
  toCompatibleKeyVector :: HasCallStack => v -> v -> H (KeyVector v)
  default toCompatibleKeyVector :: (v ~ KeyVector v) => v -> v -> H (KeyVector v)
  toCompatibleKeyVector _ = pure
  {-# INLINE toCompatibleKeyVector #-}

  unsafeBackpermuteMaybe :: Vector (Maybe IterIndex) -> v -> H (Nullable v)
  default unsafeBackpermuteMaybe :: (Nullable v ~ u (Maybe i), G.Vector u (Maybe Int), G.Vector u (Maybe i), G.Vector u i, v ~ u i)
                                 => Vector (Maybe IterIndex) -> v -> H (Nullable v)
  unsafeBackpermuteMaybe k v = pure $ unsafeBackpermuteMaybeV v (V.convert (coerce k :: V.Vector (Maybe Int)))
  {-# INLINE unsafeBackpermuteMaybe #-}

  -- (!)             :: HasCallStack => v          -> Int  -> KeyItem v
  -- {-# INLINE (!) #-}
  -- default (!) :: (G.Vector u i, v ~ u i, KeyItem v ~ i, HasCallStack) => v          -> Int  -> KeyItem v
  -- (!) = (G.!)

  toUnsafeKeyVector :: v -> KeyVector v
  default toUnsafeKeyVector :: (v ~ KeyVector v) => v -> KeyVector v
  toUnsafeKeyVector = id
  {-# INLINE toUnsafeKeyVector #-}

type HashableKey v = (Eq (KeyItem v), Hashable (KeyItem v))
type HashableDictComponent v = (HashableKey v, DictComponent v)
  
instance Iterable (Vector i)
instance (Eq i, Hashable i) => DistinctIterable (Vector i)

instance DictComponent (Vector i) where
  type KeyItem (Vector i) = i
  type Nullable (Vector i) = Vector (Maybe i)


-- * Low level

addIndexWith :: (IterIndex -> a) -> Vector c -> Vector (c, a)
addIndexWith f = V.imap $ \i b -> (b, f $ IterIndex i)
{-# INLINABLE addIndexWith #-}

buildHashMap :: (Show a, HashableDictComponent b) => (a -> a -> a) -> (IterIndex -> a) ->  b -> (HM.HashMap (KeyItem b) a)
buildHashMap g f = HM.fromListWith g . toList . addIndexWith f . toUnsafeKeyVector
{-# INLINABLE buildHashMap #-}

-- * Sorting

sortByWithM :: (Functor m, Iterable v) => (v -> m (Vector x)) -> Comparison x -> v -> m v
sortByWithM with by v = ffor (with v) $ \w -> sortByWithUnsafe w by v
{-# INLINABLE sortByWithM #-}

sortByWithUnsafe :: Iterable v => Vector x -> Comparison x -> v -> v
sortByWithUnsafe with by v = flip unsafeBackpermute v . fmap snd $ V.create $ do
  mv <- V.thaw $ addIndexWith id with
  mv <$ Intro.sortBy (on by fst) mv
{-# INLINABLE sortByWithUnsafe #-}

sortByWith :: Iterable v => (v -> Vector x) -> Comparison x -> v -> v
sortByWith with by v = sortByWithUnsafe (with v) by v
{-# INLINABLE sortByWith #-}

 
ascV :: Ord x => Vector x -> Vector x
ascV = sortV compare
{-# INLINE ascV #-}

descV :: Ord x => Vector x -> Vector x
descV = sortV $ flip compare
{-# INLINE descV #-}

-- * Grouping


-- | takes a pre-grouped vector and returns the a vector of groups
applyGrouping :: (Iterable b) => Grouping -> b -> Vector b
applyGrouping idxs v = flip unsafeBackpermute v <$> idxs
{-# INLINE applyGrouping #-}

-- | returns the vector of unique elements and a vector containing the indices for each unique element
getGroupsAndGrouping :: HashableDictComponent t => t -> (t, Grouping)
getGroupsAndGrouping vals = (unsafeBackpermute (Unsafe.head <$> idxs) vals, V.reverse . V.fromList <$> idxs)
  where idxs = V.fromList . toList $ groupIndices vals
{-# INLINABLE getGroupsAndGrouping #-}


-- | return a vector of the pre-grouped lenght containing the correspoinding group indices
broadcastGroupIndex :: Grouping -> Vector GroupIndex
broadcastGroupIndex gs = V.create $ do
  let pairs = V.concatMap sequence $ V.indexed gs
  res <- M.unsafeNew $ V.length pairs
  res <$ mapM (\(gi, IterIndex i) -> M.write res i $ GroupIndex gi) pairs

-- | takes a grouping and vector of group values and returns a vector of pre-grouped size with corresponding group values 
broadcastGroupValue :: Iterable t => Grouping -> t -> t
broadcastGroupValue gs = unsafeBackpermute $ coerce $ broadcastGroupIndex gs
{-# INLINABLE broadcastGroupValue #-}
  
-- | returns map from values to the list of indices where they appear
groupIndices :: (HashableDictComponent v) => v -> HM.HashMap (KeyItem v) [IterIndex]
groupIndices = buildHashMap collect pure
  where collect new old = case new of {[n] -> n:old; _ -> Unsafe.error "neverever"}
{-# INLINABLE groupIndices #-}

-- type ToVector f a = (Typeable f, Typeable a, Foldable f)

class ToVector f where
  toVector :: f a -> Vector a

instance Foldable f => ToVector f where toVector = V.fromList . toList
instance {-# OVERLAPS #-}ToVector Vector where toVector = id

-- toVector :: forall a f . ToVector f a => f a -> Vector a
-- toVector f | Just HRefl <- R.typeOf f `eqTypeRep` R.typeRep @(Vector a) = f
--            | True                                                       = V.fromList $ toList f
