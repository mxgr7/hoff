{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module Hoff.Dict
  (module Hoff.Dict
  ,module Hoff.Iterable
  ) where

import qualified Chronos as C
import           Control.Lens
import           Control.Monad.Writer
import           Data.Coerce
import qualified Data.HashMap.Strict as HM
import qualified Data.IntSet as IS
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.H
import           Hoff.Iterable
import           Hoff.Show
import           Hoff.Utils
import           Hoff.Vector
import qualified TextShow as B
import           TextShow hiding (fromString)
import           Yahp hiding (get, group, groupBy, (&), TypeRep, take, drop, null)

-- * Types

data Dict k v = UnsafeDict { unsafeKey          :: k
                           , unsafeValue        :: v
                           , unsafeMap          :: HM.HashMap (KeyItem k) IterIndex
                           , unsafeGroupingDict :: Dict k Grouping }
  -- deriving Functor <-- not safe, because length could be changed

makeLensesWith (lensField .~ mappingNamer (\f -> [f <> "_"]) $ lensRules) ''Dict

type DictH k v = H (Dict k v)

instance (Eq k, Eq v) => Eq (Dict k v) where
  a == b = on (==) key a b && on (==) value a b
  {-# INLINABLE (==) #-}

type VectorDict k v = Dict (Vector k) (Vector v)
type VectorDictH k v = H (VectorDict k v)
type VectorKeyDict k v = Dict (Vector k) v

-- instance (HashableDictComponent k, Monoid k, Monoid v) => Monoid (Dict k v) where
  -- mempty = UnsafeDict mempty mempty mempty

-- | this is safe, because Hashmap union has the same precedence (use second) like dictNoLengthCheck (there is a test checking this)
instance HashableKey (Vector k) => Semigroup (VectorDict k v) where
  (UnsafeDict k1 v1 m1 _) <> (UnsafeDict k2 v2 m2 _) = UnsafeDict (k1 <> k2) (v1 <> v2)
    (m1 `HM.union` ((+ IterIndex (count k1)) <$> m2)) $ getGroupingDict ks
    where ks = k1 <> k2
  {-# INLINABLE (<>) #-}

class ToDict k v a where
  toDict :: HasCallStack => a -> H (VectorDict k v)

instance ToDict k v (VectorDict k v) where toDict = pure
instance ToDict k v (H (VectorDict k v)) where toDict = id

ddh :: (ToDict k v a, ToDict k2 v2 b) => (VectorDict k v -> VectorDict k2 v2 -> H c) -> a -> b -> H c
ddh f x y = do { a <- toDict x; b <- toDict y; f a b }
{-# INLINABLE ddh #-}

(<>.) :: (HashableKey (Vector k), HasCallStack, ToDict k v a, ToDict k v b) => a -> b -> VectorDictH k v
a <>. b = (<>) <$> toDict a <*> toDict b


instance (HashableDictComponent k, DictComponent v) => Iterable (Dict k v) where
  take n = unsafeMapDict $ take n
  {-# INLINE take #-}
  count = count . key
  {-# INLINE count #-}
  null = null . key
  {-# INLINE null #-}
  drop n = unsafeMapDict $ drop n
  {-# INLINE drop #-}
  unsafeBackpermute k = unsafeMapDict $ unsafeBackpermute k
  {-# INLINE unsafeBackpermute #-}

zipDict :: VectorDict k v -> Vector (k, v)
zipDict d = V.zip (key d) $ value d 
{-# INLINABLE zipDict #-}

zipDictWithM :: Monad m => (k -> v -> m a) -> VectorDict k v -> m (Vector a) 
zipDictWithM f d = V.zipWithM f (key d) $ value d 
{-# INLINABLE zipDictWithM #-}

zipDictWith :: (k -> v -> a) -> VectorDict k v -> Vector a
zipDictWith f d = V.zipWith f (key d) $ value d 
{-# INLINABLE zipDictWith #-}

unsafeMapDictValue :: (HashableDictComponent k, DictComponent a, DictComponent b) => (a -> b) -> Dict k a -> Dict k b
unsafeMapDictValue = (unsafeValue_ %~)
{-# INLINABLE unsafeMapDictValue #-}

mapDictValues :: (HashableDictComponent k, Functor f, DictComponent (f a), DictComponent (f b))
  => (a -> b) -> Dict k (f a) -> Dict k (f b)
mapDictValues = unsafeMapDictValue . fmap
{-# INLINABLE mapDictValues #-}

unsafeMapDictKey :: (HashableDictComponent k, HashableDictComponent k2, DictComponent v) => (k -> k2) -> Dict k v -> Dict k2 v
unsafeMapDictKey f v = dictNoLengthCheck (f $ key v) $ value v
{-# INLINABLE unsafeMapDictKey #-}

mapDictKeys :: (Functor f, HashableDictComponent (f k), HashableDictComponent (f k2), DictComponent v)
  => (k -> k2) -> Dict (f k) v -> Dict (f k2) v
mapDictKeys = unsafeMapDictKey . fmap
{-# INLINABLE mapDictKeys #-}

unsafeMapDict :: (HashableDictComponent k, DictComponent v) => (forall a . Iterable a => a -> a) -> Dict k v -> Dict k v
unsafeMapDict f v = dictNoLengthCheck (f $ key v) $ f $ value v
{-# INLINABLE unsafeMapDict #-}

-- * construction

dictV :: forall k v f g . (HasCallStack, ToVector f, ToVector g, HashableKey (Vector k)) => f k -> g v -> VectorDictH k v
dictV k = dict (toVector k) . toVector
{-# INLINABLE dictV #-}

dict :: (HasCallStack, HashableDictComponent k, DictComponent v) => k -> v -> DictH k v
dict k v = requireSameCount k v "in dict" $ dictNoLengthCheck k v
{-# INLINABLE dict #-}

dictNoLengthCheck :: (HashableDictComponent k, DictComponent v) => k -> v -> Dict k v
dictNoLengthCheck k v = UnsafeDict k v (buildHashMap keepFirst id k) $ getGroupingDict k
  where keepFirst _ v = v
{-# INLINABLE dictNoLengthCheck #-}

requireSameCount :: (HasCallStack, DictComponent v1, DictComponent v2) => v1 -> v2 -> String -> p -> H p
requireSameCount a b msg f = f <$ guardH (CountMismatch msg) (count a == count b)
{-# INLINABLE requireSameCount #-}

-- * operations

key :: Dict k v -> k
key   = unsafeKey
{-# INLINABLE key #-}

value :: Dict k v -> v
value = unsafeValue
{-# INLINABLE value #-}


(!) :: (HasCallStack, HashableKey k, Show (KeyItem k)) => Dict k (Vector v) -> KeyItem k -> H v
d ! k = noteH (KeyNotFound $ show k) $ d !? k
{-# INLINABLE (!) #-}

-- | todo: I do not think this is safe for a keyedtable
(!?) :: (HasCallStack, HashableKey k, Show (KeyItem k)) => Dict k (Vector v) -> KeyItem k -> Maybe v 
d !? k = (value d V.!) . fromIterIndex <$> unsafeMap d HM.!? k
{-# INLINABLE (!?) #-}

(!!) :: (HasCallStack, Typeable k, DictComponent v, Hashable k, Eq k, Show k) => Dict (Vector k) v -> Vector k -> H v
d !! k = flip unsafeBackpermute (value d) <$> safeMapAccess g k d
  where g hm = mapM $ \k -> noteH (KeyNotFound $ show (typeOf k) <> " " <> show k) $ hm HM.!? k
{-# INLINABLE (!!) #-}

(!!/) :: (HasCallStack, Typeable (KeyItem k), DictComponent v, HashableDictComponent k, Show (KeyItem k)) => Dict k v -> k -> H v
d !!/ k = flip unsafeBackpermute (value d) <$> join (safeMapAccessH g k d)
  where g hm = mapM $ \k -> noteH (KeyNotFound $ show (typeOf k) <> " " <> show k) $ hm HM.!? k
{-# INLINABLE (!!/) #-}

safeMapAccessH :: (HasCallStack, DictComponent k) => (HM.HashMap (KeyItem k) IterIndex -> KeyVector k ->  a) -> k -> Dict k v -> H a
safeMapAccessH f k d = f (unsafeMap d) <$> toCompatibleKeyVector (key d) k
{-# INLINABLE safeMapAccessH #-}

safeMapAccess :: (HM.HashMap k IterIndex -> Vector k ->  a) -> Vector k -> VectorKeyDict k v -> a
safeMapAccess f k d = f (unsafeMap d) k
{-# INLINABLE safeMapAccess #-}


-- | the implementation makes sure that the order is conserverd
-- this is important to get comparable table rows for Eq TableKey
intersectAndDiff :: (HasCallStack, Hashable k, Eq k, DictComponent v) =>
  Vector k -> VectorKeyDict k v -> (VectorKeyDict k v, VectorKeyDict k v)
intersectAndDiff s d = (unsafeBackpermute intersect d, unsafeRemoveByIndex d intersect)
  where intersect = safeMapAccess (\hm -> V.mapMaybe (hm HM.!?)) s d
{-# INLINABLE intersectAndDiff #-}

-- | the implementation makes sure that the order is conserverd
-- this is important to get comparable table rows for Eq TableKey
intersectAndDiffH :: (HasCallStack, HashableDictComponent k, DictComponent v) => k -> Dict k v -> H (Dict k v, Dict k v)
intersectAndDiffH s d = ffor (safeMapAccessH (\hm -> V.mapMaybe (hm HM.!?)) s d)
  $ \intersect -> (unsafeBackpermute intersect d, unsafeRemoveByIndex d intersect)
{-# INLINABLE intersectAndDiffH #-}


(!!?) :: (HasCallStack, Hashable k, Eq k) => VectorDict k v -> Vector k -> Vector (Maybe v)
d !!? k = unsafeBackpermuteMaybeV (value d) $ coerce $ safeMapAccess (\hm -> fmap (hm HM.!?)) k d
{-# INLINABLE (!!?) #-}

(!!?/) :: (HasCallStack, DictComponent v, HashableDictComponent k) => Dict k v -> k -> H (Nullable v)
d !!?/ k = flip unsafeBackpermuteMaybe (value d) =<< safeMapAccessH (\hm -> fmap (hm HM.!?)) k d
{-# INLINABLE (!!?/) #-}


-- | returns a subset of items from the first argment which are missing in the second
-- (in unspecified order)
missing :: (HasCallStack, Hashable k, Eq k) => Vector k -> VectorKeyDict k v -> Vector k
missing = safeMapAccess $ \hm -> V.filter $ not . flip HM.member hm
{-# INLINABLE missing #-}

-- | returns a subset of items from the first argment which are missing in the second
-- (in unspecified order)
missingH :: (HasCallStack, HashableDictComponent k) => k -> Dict k v -> H (Vector (KeyItem k))
missingH = safeMapAccessH $ \hm -> V.filter $ not . flip HM.member hm
{-# INLINABLE missingH #-}


unsafeRemoveByIndex :: (HashableDictComponent k, DictComponent v, Foldable f) => Dict k v -> f IterIndex -> Dict k v
unsafeRemoveByIndex d ks = unsafeBackpermute diff d
  where diff = V.filter (\i -> not $ IS.member (coerce i) lookup) $ V.enumFromN 0 (count $ key d)
        lookup = IS.fromList $ coerce $ toList ks
{-# INLINABLE unsafeRemoveByIndex #-}

-- * fill

untouched :: p1 -> p2 -> p2
untouched _ x = x

keepFirst :: p1 -> p2 -> p3 -> p2
keepFirst _ x _ = x

keepSecond :: p1 -> p2 -> p3 -> p3
keepSecond _ _ x = x 

untouchedA :: Applicative f => p -> a -> f a
untouchedA _ x = pure x

keepFirstA :: Applicative f => p1 -> a -> p2 -> f a
keepFirstA _ x _ = pure x

keepSecondA :: Applicative f => p1 -> p2 -> a -> f a
keepSecondA _ _ x = pure x

-- | fill missing keys of the second dict with those from the first
-- 
-- this is the same as merging the dicts and using the second dict's values for common keys
fillDict :: HashableKey (Vector k) => VectorDict k v -> VectorDict k v -> VectorDict k v
fillDict = fillDictWith untouched untouched keepSecond
{-# INLINABLE fillDict #-}

mapMDictWithKey :: (Monad m, HashableDictComponent k) => (KeyItem k -> v -> m w) -> Dict k (Vector v) -> m (Dict k (Vector w))
mapMDictWithKey f d = dictNoLengthCheck (key d) <$> V.zipWithM f (toUnsafeKeyVector $ key d) (value d)
{-# INLINABLE mapMDictWithKey #-}

mapDictWithKey :: (HashableDictComponent k) => (KeyItem k -> v -> w) -> Dict k (Vector v) -> Dict k (Vector w)
mapDictWithKey f d = dictNoLengthCheck (key d) $ V.zipWith f (toUnsafeKeyVector $ key d) $ value d
{-# INLINABLE mapDictWithKey #-}


fillDictWithM :: forall a b c k m. (Monad m, HasCallStack, HashableKey (Vector k))
  => (k -> a -> m c) -> (k -> b -> m c) -> (k -> a -> b -> m c) -> VectorDict k a -> VectorDict k b -> m (VectorDict k c)
fillDictWithM fa fb fab a b = do
  (a', intersect) <- safeMapAccess (\hmB -> runWriterT . V.zipWithM (combine hmB) (value a)) (key a) b
  (a { unsafeValue = a' } <>) <$> mapMDictWithKey fb (unsafeRemoveByIndex b intersect)
  where combine hmB vA kA = maybe (lift $ fa kA vA) (both kA) $ hmB HM.!? kA :: WriterT [IterIndex] m c
          where both kA idx = tell [idx] >> lift (fab kA vA $ value b V.! fromIterIndex idx)
{-# INLINABLE fillDictWithM #-}

-- | todo: simpler implementation: similar to how ujk works, get unique key vector first and then fill values accordingly 
fillDictWith :: forall a b c k . (HasCallStack, HashableKey (Vector k))
  => (k -> a -> c) -> (k -> b -> c) -> (k -> a -> b -> c) -> VectorDict k a -> VectorDict k b -> VectorDict k c
fillDictWith fa fb fab a b = a { unsafeValue = a' } <> (mapDictWithKey fb $ unsafeRemoveByIndex b intersect)
  where (a', intersect) = safeMapAccess (\hmB -> runWriter . V.zipWithM (combine hmB) (value a)) (key a) b
        combine hmB vA kA = maybe (pure $ fa kA vA) (both kA) $ hmB HM.!? kA
          where both kA idx = tell [idx] >> pure (fab kA vA (value b V.! fromIterIndex idx)) :: Writer [IterIndex] c
{-# INLINABLE fillDictWith #-}

deleteKeys :: (Hashable k, Eq k, DictComponent v) => Vector k -> VectorKeyDict k v -> VectorKeyDict k v
deleteKeys = fmap2 snd intersectAndDiff
{-# INLINABLE deleteKeys #-}

deleteKeysH :: (HashableDictComponent k, DictComponent v) => k -> Dict k v -> DictH k v
deleteKeysH = fmap3 snd intersectAndDiffH
{-# INLINABLE deleteKeysH #-}

ascD :: (HashableDictComponent k, DictComponent x, Ord x) =>
        Dict k (Vector x) -> Dict k (Vector x)
ascD = sortD compare
{-# INLINE ascD #-}

descD :: (HashableDictComponent k, DictComponent x, Ord x) =>
         Dict k (Vector x) -> Dict k (Vector x)
descD = sortD $ flip compare
{-# INLINE descD #-}

sortD :: (HashableDictComponent k, DictComponent x)
      => Comparison x -> Dict k (Vector x) -> Dict k (Vector x)
sortD = sortByWith value
{-# INLINE sortD #-}

-- requireKeyCompatibility :: IteraHasCallStack => v -> v
-- requireKeyCompatibility a b v = maybe v (throwH . IncompatibleKeys . toS) $ checkKeyCompatibility a b


groupBy_ :: (DictComponent t, HashableDictComponent g) => g -> t -> Dict g (Vector t)
groupBy_ g = dictNoLengthCheck gu . applyGrouping idxs
  where (gu, idxs) = getGroupsAndGrouping g
{-# INLINABLE groupBy_ #-}
  

-- | returns the vector of unique elements and a vector containing the indices for each unique element
getGroupingDict :: HashableDictComponent t => t -> Dict t Grouping
getGroupingDict = uncurry dictNoLengthCheck . getGroupsAndGrouping
{-# INLINABLE getGroupingDict #-}

-- * show

instance {-# OVERLAPPING #-} AtomShow Bool where
  buildAtomRaw      = singleton . bool '0' '1'
  buildSingleLineVector p = buildSingleton p . fold
  buildSingleton        _ = (<> singleton 'b')
  showNothing _ = "_"

instance {-# OVERLAPPING #-} AtomShow Int64 where
  buildSingleLineVector _ = unwordsB . toList
  showNothing _ = "0N"

instance {-# OVERLAPPING #-} AtomShow Int where
  buildSingleLineVector p = buildSingleton p . unwordsB . toList
  buildSingleton        _ = (<> singleton 'i')
  showNothing _ = "0N"

instance {-# OVERLAPPING #-} AtomShow Char where
  buildAtomRaw = singleton
  buildSingleLineVector _ v = singleton '"' <> fold v <> singleton '"'
  showNothing _ = "☐"


instance {-# OVERLAPPING #-} AtomShow None where

instance {-# OVERLAPPING #-} AtomShow Double where
  buildSingleLineVector _ = unwordsB . toList
  showNothing _ = "0n"

instance {-# OVERLAPPING #-} AtomShow C.Day where
  buildSingleLineVector _ = unwordsB . toList
  buildAtomRaw = C.builder_Ymd (Just '.') . C.dayToDate
  showNothing _ = "0d"
  
instance {-# OVERLAPPING #-} AtomShow C.Time where
  buildSingleLineVector _ = unwordsB . toList
  buildAtomRaw = C.builderIso8601 . C.timeToDatetime
  showNothing _ = "0t"
  
instance {-# OVERLAPPING #-} AtomShow String where
  buildAtomRaw = B.fromString
  buildSingleLineVector _ = buildSingleLineVector (Proxy @Text)
  buildSingleton _ = buildSingleton (Proxy @Text)
  showNothing _ = showNothing (Proxy @Text)
  
instance {-# OVERLAPPING #-} AtomShow Text where
  buildAtomRaw = fromText
  buildSingleLineVector = V.foldMap . buildSingleton
  buildSingleton _ = (singleton '`' <>)
  

vectorProxy :: Vector (g a) -> Proxy a
vectorProxy _ = Proxy

instance AtomShow a => ValueShow (Vector a) where
  buildSingleLine = buildSingleLineVector (Proxy @a) . fmap buildAtomRaw
  {-# INLINE buildSingleLine #-}
  showv = if defaultIsSingleLine (Proxy @a) then toS . toLazyText . buildSingleLine
    else unlinesV . buildSingleColumn
  {-# INLINABLE showv #-}

instance AtomShow a => DictComponentShow (Vector a) where
  getDictBuilder = singleColumnInDict Nothing
  {-# INLINABLE getDictBuilder #-}

instance AtomShow a => SingleColumnShow (Vector a) where
  buildSingleColumn = fmap buildAtomInColumn
  {-# INLINE buildSingleColumn #-}

instance (DictComponentShow a, DictComponentShow b) => ValueShow (Dict a b) where
  buildSingleLine d = buildSingleLine (key d) <> singleton '!' <> buildSingleLine (value d) 
  {-# INLINABLE buildSingleLine #-}
  showv d = unlinesV $ combine $ horizontalConcat "| " $ trans <$> (keys :| [vals])
    where (keys, vals) = (getDictBuilder $ key d, getDictBuilder $ value d)
          trans x = InDictBuilder 0 "" False $ combine $ x { hNonEmtpyHeader = on (||) hNonEmtpyHeader keys vals }
  {-# INLINABLE showv #-}

instance (DictComponentShow a, DictComponentShow b) => Show (Dict a b) where show = toS . showv
