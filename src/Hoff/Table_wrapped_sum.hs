{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UndecidableInstances #-}
module Hoff.Table
  (module Hoff.Table
  ,module Reexport
  ) where

import qualified Chronos as C
import           Control.Exception
import           Control.Lens
import           Data.Coerce
import           Data.Functor.Classes as Reexport
import qualified Data.HashMap.Strict as HM
import qualified Data.HashTable.ST.Cuckoo as HT
import           Data.Hashable
import           Data.Hashable.Lifted (Hashable1(..), hashWithSalt1)
import qualified Data.List
import qualified Data.Maybe
import           Data.Record.Anon as Reexport (KnownFields, pattern (:=), K(..), AllFields, RowHasField)
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Record.Anon.Advanced as R
import           Data.SOP
import           Data.String
import           Data.Type.Equality
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as G
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.Show
import           Hoff.Utils
import           Hoff.Vector
import qualified Prelude
import           TextShow as B
import           Type.Reflection as R
import           Unsafe.Coerce ( unsafeCoerce )
import           Yahp as P hiding (get, group, groupBy, (&), TypeRep, typeRep, Hashable, hashWithSalt, (:.:), null)


-- TODO: do not do unnecessary bound checks


class (Eq a, Typeable a, Show a, Hashable a, AtomShow a, Ord a) => Wrappable a
instance (Eq a, Typeable a, Show a, Hashable a, AtomShow a, Ord a) => Wrappable a
  
type WrapCoerce f g a = Data.Coerce.Coercible (f (g a)) (f (WrapInstances g a))
type UnWrapCoerce f g a = Data.Coerce.Coercible (f (WrapInstances g a)) (f (g a))
type Wrappable1 a = (Eq1 a, Hashable1 a, AtomShow1 a, Typeable a, Ord1 a)
type Wrappable2 g a = (Wrappable a, Wrappable1 g)
type Wrappable2' a = (Eq a, Typeable a, Show a, Hashable a, Ord a) -- atomshow(1)?
type WrapInstancesClass a = (Eq a, Hashable a, Ord a)

newtype WrapInstances g a = WrapInstances { unwrapInstances :: (g a) }

instance (Wrappable2 g a) => Eq (WrapInstances g a) where
  (WrapInstances a) == (WrapInstances b) = eq1 a b
  {-# INLINE (==) #-}

instance (Wrappable2 g a) => Hashable (WrapInstances g a) where
  hashWithSalt s = hashWithSalt1 s . unwrapInstances
  {-# INLINE hashWithSalt #-}

instance (Wrappable2 g a) => Ord (WrapInstances g a) where
  compare (WrapInstances a) (WrapInstances b) = compare1 a b
  {-# INLINE compare #-}

newtype N x = N { unN :: x }
newtype S x = S { unS :: x }

errS :: forall x a . Typeable x => String -> S x -> a
errS msg _ = Prelude.error $ msg <> " not implemented for S (" <> show (R.typeRep @x) <> ")"

instance Typeable x => Eq (S x) where x == _ = errS "Eq" x
instance Typeable x => Ord (S x) where compare = errS "Ord"
instance Show x => Show (S x) where show = show . unS
instance Typeable x => Hashable (S x) where hashWithSalt _ = errS "Hashable"

errN :: forall x a . Typeable x => String -> N x -> a
errN msg _ = Prelude.error $ msg <> " not implemented for N (" <> show (R.typeRep @x) <> ")"

instance Typeable x => Eq (N x) where x == _ = errN "Eq" x
instance Typeable x => Ord (N x) where compare = errN "Ord"
instance Typeable x => Show (N x) where show = errN "Show"
instance Typeable x => Hashable (N x) where hashWithSalt _ = errN "Hashable"

coerceUnWrapInstances :: UnWrapCoerce f g a => f (WrapInstances g a) -> f (g a)
coerceUnWrapInstances = coerce
{-# INLINE coerceUnWrapInstances #-}

coerceWrapInstances :: WrapCoerce f g a => f (g a) -> f (WrapInstances g a)
coerceWrapInstances = coerce
{-# INLINE coerceWrapInstances #-}

data WrappedDyn f = forall a. (Wrappable a) => WrappedI (TypeRep (I a))         !(f (I a))
                  | forall a. (Wrappable a) => WrappedM (TypeRep (Maybe a))     !(f (Maybe a))
                  | WrappedN                                                    !(f (I None))

makeLensesWith (lensField .~ mappingNamer (\f -> [f <> "_"]) $ lensRules) ''WrappedDyn

type Symbol = Text
type Symbols = Vector Text

type TableCol' f = WrappedDyn f
type TableCol = WrappedDyn Vector
type TableCell = WrappedDyn I
type TableDict' f = Dict Symbols (Vector (TableCol' f))
type TableDict = Dict Symbols (Vector TableCol)
type TableRowDict = Dict Symbols (Vector TableCell)
type TableR = Record Vector
type KeyedTableR k v = (Record Vector k, Record Vector v)

newtype Table' f = UnsafeTable { unsafeTable :: TableDict' f }
newtype TableH' f = H (Table' f)
type Table = Table' Vector
type GroupedCol = WrappedDyn (Vector :.: Vector)
type GroupedTableRaw = Table' (Vector :.: Vector)
type GroupedTable = Dict Table GroupedTableRaw
type KeyedTable = Dict Table Table
type SingleKeyTable a = Dict (Vector a) Table

makeLensesWith (lensField .~ mappingNamer (\f -> [f <> "_"]) $ lensRules) ''Table'


class FromTable a where
  fromTable :: HasCallStack => Table -> a

class ToTable a where
  toTable :: HasCallStack => a -> Table

instance ToTable Table where toTable = id
instance ToTable KeyedTable where toTable =undefined -- unkey

instance Hashable1 I where
    liftHashWithSalt h salt (I x) = h salt x
  
-- * from/to WrappedDyns

-- | convenience: accepts (f a) instead of (f (I a))
toWrappedDynI :: (ICoerce f a, Wrappable a) => f a -> WrappedDyn f
toWrappedDynI = WrappedI typeRep . coerceI
{-# INLINABLE toWrappedDynI #-}

-- | the full blown version (which has an inverse: fromWrappedDynF (up to I None ~ Maybe a equivalence))
class Wrappable3 f a where
  toWrappedDyn :: f a -> WrappedDyn f

instance Wrappable a => Wrappable3 f (Maybe a) where 
  toWrappedDyn = WrappedM typeRep
  {-# INLINE toWrappedDyn #-}

instance Wrappable a => Wrappable3 f (I a) where 
  toWrappedDyn = WrappedI typeRep
  {-# INLINE toWrappedDyn #-}

-- | convenience: unwraps (I a) to a
fromWrappedDyn :: forall a . (Typeable a, HasCallStack) => TableCol -> Vector a
fromWrappedDyn (WrappedI t v)
    | Just HRefl <- t     `eqTypeRep` typeRep @a                  = v
fromWrappedDyn w = fromWrappedDynF w
{-# INLINABLE fromWrappedDyn #-}

-- class Replicate f where replicate' :: Int -> a -> f a
-- instance Replicate Vector where replicate' = V.replicate

-- | the full blown version (which has an inverse: toWrappedDyn (up to I None ~ Maybe a equivalence))
fromWrappedDynF :: forall a . (Typeable a, HasCallStack) => TableCol -> Vector a
fromWrappedDynF = \case
  WrappedI t v
    | Just HRefl        <- t    `eqTypeRep` ta  -> v
    | True -> err t
  WrappedM t v
    | Just HRefl        <- t    `eqTypeRep` ta  -> v
    | True -> err t
  WrappedN v
    | App con _         <- ta
    , Just HRefl        <- con  `eqTypeRep` typeRep @Maybe      -> V.replicate (V.length v) Nothing 
    | True -> err $ typeRep @None
  where ta = typeRep @a
        err t = throw $ TypeMismatch $ "Cannot convert " <> show t <> " to " <> show (typeRep @a)
        err :: TypeRep k -> b
{-# INLINABLE fromWrappedDynF #-}

withWrapped :: (forall a g. (Wrappable2 g a) => f (g a) -> b) -> WrappedDyn f -> b
withWrapped f = \case
  WrappedI _ wd -> f wd
  WrappedM _ wd -> f wd
  WrappedN wd   -> f wd
{-# INLINE withWrapped #-}

withWrapped' :: (forall a g. (Wrappable2 g a) => TypeRep (g a) -> f (g a) -> b) -> WrappedDyn f -> b
withWrapped' f = \case
  WrappedI tr wd -> f tr wd
  WrappedM tr wd -> f tr wd
  WrappedN wd   -> f (typeRep @(I None)) wd
{-# INLINE withWrapped' #-}

mapTableCol :: WrapCoerce Vector g a => (forall a . WrapInstancesClass a => Vector a -> Vector a) -> TableCol -> TableCol
mapTableCol f = mapWrapped $ coerceUnWrapInstances . f . coerceWrapInstances
{-# INLINE mapTableCol #-}

mapWrapped :: (forall g a. (Wrappable2 g a) => f (g a) -> h (g a)) -> WrappedDyn f -> WrappedDyn h
mapWrapped f = \case
  WrappedI tr wd        -> WrappedI tr $ f wd
  WrappedM tr wd        -> WrappedM tr $ f wd
  WrappedN wd           -> WrappedN    $ f wd
{-# INLINE mapWrapped #-}

-- mapWrappedD :: (Functor f, Wrappable b, Wrappable1 k) => (f b -> h (k b)) -> WrappedDyn f -> WrappedDyn h
-- mapWrappedD f = toWrappedDyn . f . fromWrappedDyn

-- instance Show (WrappedDyn f) where show = withWrapped show

flipTable :: Table' f -> TableDict' f
flipTable = unsafeTable
{-# INLINE flipTable #-}

cols :: Table' f -> Symbols
cols = key . flipTable

vals :: Table' f -> Vector (TableCol' f)
vals = value . flipTable

-- | no bounds checking
unsafeRowDict :: Table -> Int -> TableRowDict
unsafeRowDict t i = dictNoLengthCheck (cols t) $ withWrapped (toWrappedDyn . I . (flip V.unsafeIndex i)) <$> vals t

data TableKey' f        = UnsafeTableKey { tTable :: Table' f
                                         , tIdx   :: {-# UNPACK #-} !IterIndex
                                         }

type TableKey = TableKey' Vector

instance Show TableKey where
  show (UnsafeTableKey _ i) = "TableKey " <> show i

-- * construction
  
-- table :: HasCallStack => [(Symbol, TableCol)] -> Table
-- table = table2 . unsafeRunH . uncurry dict . V.unzip . V.fromList 

-- tableAnonymous :: HasCallStack => [(Maybe Symbol, TableCol)] -> Table
-- tableAnonymous = table2 . (\(k,v) -> dict (V.fromList $ makeUniqueNames k) $ V.fromList v) . unzip

-- -- | inspired by q:
-- -- 1. unnamed columns get called x
-- -- 2. duplicate columns get suffixed by an number
-- -- 3. this function can produce duplicates when the input already contains number-suffixed columns, e.g. [a,a1] -> [a1,a1]
-- makeUniqueNames :: [Maybe Symbol] -> [Symbol]
-- makeUniqueNames cols = runST $ do
--   ht <- HT.newSized $ length cols
--   let appendNumber name = HT.mutate ht name $ \case
--         Nothing -> (Just (1 :: Int), name)
--         Just c  -> (Just $ succ c, name <> showt c)
--   forM cols $ appendNumber . fromMaybe "x"


-- table2 :: HasCallStack => TableDict -> H Table
-- table2 d = case equalLength $ mapDictValue (fmap $ withWrapped V.length) d of
--   Just (_, Just err) -> throw $ TableDifferentColumnLenghts err
--   Nothing        -> throw $ TableWithNoColumn "not allowed"
--   _              -> pure $ UnsafeTable d

-- equalLength :: VectorDict Symbol Int -> Maybe (Int, Maybe String)
-- equalLength d = fmap (\(c, cs) -> seq c $ (c, err <$ guard (V.any (/= c) cs))) $ V.uncons $ value d
--   where err = show d

-- tc :: forall a f . (ToVector f, Wrappable a) => Symbol -> f a -> Table
-- tc name v = table [(name, toWrappedDynI $ toVector v)] 
-- {-# INLINABLE tc #-}

-- tcF :: (ToVector h, Wrappable a, Wrappable1 f) => Symbol -> h (f a) -> Table
-- tcF name v = table [(name, toWrappedDyn $ toVector v)]
-- {-# INLINABLE tcF #-}

-- (<#) :: (ToVector f, Wrappable a) => Symbol -> f a -> Table
-- (<#) = tc
-- {-# INLINE (<#) #-}
  

-- -- * instances 

-- instance Eq Table where
--   (==) = on (==) flipTable

-- -- eqt :: Table -> Table -> Table
-- -- eqt | 

instance Eq TableCol where
  (WrappedI t1 v1) == (WrappedI t2 v2)
    | Just HRefl <- t1 `eqTypeRep` t2   = v1 `eq1` v2 -- on (==) coerceWrapInstances v1 v2
    |  True                             = False
  (WrappedM t1 v1) == (WrappedM t2 v2)
    | Just HRefl <- t1 `eqTypeRep` t2   = v1 `eq1` v2
    |  True                             = False
  (WrappedN v1) == (WrappedN v2) = on (==) V.length v1 v2
  _ == _ = False

-- | careful: this instance assumes that key t1 == key t2, and typeOf for the column vectors
instance Eq TableKey where
  UnsafeTableKey t1 (IterIndex i1) == UnsafeTableKey t2 (IterIndex i2) = V.and $ on (V.zipWith compareCell) vals t1 t2  
    where compareCell (WrappedI _ v1) (WrappedI _ v2)   = V.unsafeIndex v1 i1 `eq1` unsafeCoerce (V.unsafeIndex v2 i2)
          compareCell (WrappedM _ v1) (WrappedM _ v2)   = V.unsafeIndex v1 i1 `eq1` unsafeCoerce (V.unsafeIndex v2 i2)
          compareCell (WrappedN _ _) (WrappedN _ _)     = True
          compareCell _ _                               = P.error "this should never happen"
            -- | Just HRefl <- t1 `eqTypeRep` t2 = v1 V.! i1 == v2 V.! i2
            -- | True                            = False 
          compareCell :: TableCol -> TableCol -> Bool

-- instance Hashable TableKey where
--   hashWithSalt salt (UnsafeTableKey t (IterIndex i)) = hashWithSaltFold salt
--     $ \hash -> (withWrapped $ \cv -> hash $ V.unsafeIndex cv i) <$> vals t


-- hashWithSaltFold :: Foldable f => Int -> (forall b . (forall a g . (Hashable1 g, Hashable a) => (g a) -> b) -> f b) -> Int
-- hashWithSaltFold salt g = foldl' (&) salt $ g $ flip hashWithSalt1
-- {-# INLINE hashWithSaltFold #-}

-- -- hashWithSaltFold :: Foldable f => Int -> (forall b . (forall x . Hashable x => x -> b) -> f b) -> Int
-- -- hashWithSaltFold salt g = foldl' (\b a -> a b) salt $ g $ flip hashWithSalt

-- instance Iterable Table where
--   count = maybe (throw $ TableWithNoColumn "count not defined") (withWrapped V.length) . (V.!? 0) . vals

--   null = maybe (throw $ TableWithNoColumn "null not defined") (withWrapped V.null) . (V.!? 0) . vals

--   take n = mapTableWrapped $ H.take n

--   drop n = mapTableWrapped $ H.drop n

--   unsafeBackpermute ks = mapTableWrapped $ flip G.unsafeBackpermute $ coerce ks

-- instance DistinctIterable Table where
--   distinct t | count (cols t) == 1 = mapTable (mapTableCol distinctV) t
--              | True                = unsafeBackpermute (coerce $ distinctVIndices $ toUnsafeKeyVector t) t
    

-- -- instance Iterable (Table' f) => DictComponent (Table' f) where
-- --   type KeyItem (Table' f)= TableKey' f
-- --   type Nullable (Table' f) = (Table' f)

-- instance Iterable Table => DictComponent Table where
--   type KeyItem Table= TableKey
--   type Nullable Table = Table



--   -- {-# INLINE (!) #-}
--   -- (!)             :: HasCallStack => Table          -> Int  -> TableKey
--   -- t ! idx = A.map (\col -> I $ col V.! idx) t 
--   unsafeBackpermuteMaybe = mapTableWithName . unsafeBackpermuteMaybeTableCol


--   toUnsafeKeyVector t = V.generate (count t) (UnsafeTableKey t . IterIndex)

--   toCompatibleKeyVector that this = toUnsafeKeyVector $ orderedColumnIntersection False (\_ _ a -> a) that this
          

-- -- | returns a subset of columns of the second table with column order and types as the first
-- -- the columns resulting columns are combined with the columns of the first table using the given combination function
-- orderedColumnIntersection :: HasCallStack => Bool ->
--   (forall g a . Wrappable2 g a => Vector (g a) -> Vector (g a) -> TableCol -> TableCol)
--   -> Table -> Table -> Table
-- orderedColumnIntersection requireIdenticalColumnSet combine sourceTable targetTable
--   | Prelude.null allErrs        = table2 $ dict requiredCols
--                                   $ zipTable3 combine2 sourceTable subTargetVals
--   | True                        = throw $ IncompatibleTables $ Prelude.unlines $ allErrs
--   where (subTargetVals, additionalCols) = first value $ intersectAndDiff requiredCols targetCols
--         combine2 name (WrappedDyn t1 v1) w2@(WrappedDyn t2 v2)
--           | Just HRefl <- t1 `eqTypeRep` t2   = combine v1 v2 w2
--           | True                              = throw $ TypeMismatch
--             $ "Column " <> toS name <> ": " <> show t1 <> " /=  " <> show t2
--         requiredCols = cols sourceTable
--         targetCols = flipTable targetTable
--         allErrs = catMaybes
--           [("Missing from target table: " <> show (missing requiredCols targetCols))
--            <$ guard (count requiredCols /= count subTargetVals)
--           ,("Missing from source table: " <> show (key additionalCols))
--            <$ guard (requireIdenticalColumnSet && not (null additionalCols))
--           ]
-- {-# INLINABLE orderedColumnIntersection #-}
                                

-- instance Semigroup Table where
--   (<>) = fmap table2 . on (<>) flipTable

-- -- * Show

-- instance (KnownFields r, AllFields r (Compose Show I)) => TextShow (Record I r) where
--   showbPrec _ = fromText . toS . show
--   {-# INLINE showbPrec #-}
  
-- instance {-# OVERLAPPING #-} (KnownFields r, AllFields r (Compose Show I)) => AtomShow (Record I r) where
--   buildSingleLineVector _ = unwordsB . toList
--   {-# INLINE buildSingleLineVector #-}

-- instance {-# OVERLAPPING #-} AtomShow Bool where
--   buildAtomRaw      = singleton . bool '0' '1'
--   buildSingleLineVector p = buildSingleton p . fold
--   buildSingleton        _ = (<> singleton 'b')
--   showNothing _ = "_"

-- instance {-# OVERLAPPING #-} AtomShow Int64 where
--   buildSingleLineVector _ = unwordsB . toList
--   showNothing _ = "0N"

-- instance {-# OVERLAPPING #-} AtomShow Int where
--   buildSingleLineVector p = buildSingleton p . unwordsB . toList
--   buildSingleton        _ = (<> singleton 'i')
--   showNothing _ = "0N"

-- instance {-# OVERLAPPING #-} AtomShow Char where
--   buildAtomRaw = singleton
--   buildSingleLineVector _ v = singleton '"' <> fold v <> singleton '"'
--   showNothing _ = "☐"


-- instance {-# OVERLAPPING #-} AtomShow None where

-- instance {-# OVERLAPPING #-} AtomShow Double where
--   buildSingleLineVector _ = unwordsB . toList
--   showNothing _ = "0n"

-- instance {-# OVERLAPPING #-} AtomShow C.Day where
--   buildSingleLineVector _ = unwordsB . toList
--   buildAtomRaw = C.builder_Ymd (Just '.') . C.dayToDate
--   showNothing _ = "0d"
  
-- instance {-# OVERLAPPING #-} AtomShow C.Time where
--   buildSingleLineVector _ = unwordsB . toList
--   buildAtomRaw = C.builderIso8601 . C.timeToDatetime
--   showNothing _ = "0t"
  
-- instance {-# OVERLAPPING #-} AtomShow String where
--   buildAtomRaw = B.fromString
--   buildSingleLineVector _ = buildSingleLineVector (Proxy @Text)
--   buildSingleton _ = buildSingleton (Proxy @Text)
--   showNothing _ = showNothing (Proxy @Text)
  
-- instance {-# OVERLAPPING #-} AtomShow Text where
--   buildAtomRaw = fromText
--   buildSingleLineVector = V.foldMap . buildSingleton
--   buildSingleton _ = (singleton '`' <>)
  
-- instance {-# OVERLAPPING #-} AtomShow TableCell where
--   buildAtomRaw = withWrapped (\(I (x :: g a)) -> buildSingleton (Proxy @a) $ buildAtomRaw1 x)
--   defaultIsSingleLine _ = False

-- -- instance {-# OVERLAPPING #-} AtomShow GroupedCol where
--   -- buildAtomRaw = buildSingleLine
--   -- defaultIsSingleLine _ = False

-- instance {-# OVERLAPPING #-} AtomShow TableCol where
--   buildAtomRaw = buildSingleLine
--   defaultIsSingleLine _ = False

-- -- instance SingleColumnShow GroupedCol where
--   -- buildSingleColumn = undefined -- withWrapped $ fmap buildSingleLine . unComp

-- instance SingleColumnShow TableCol where
--   buildSingleColumn = withWrapped $ fmap buildAtomInColumn1

-- instance ValueShow TableCol where
--   buildSingleLine = withWrapped $ \v ->
--     buildSingleLineVector (vectorProxy v) $ buildAtomRaw1 <$> v
--   showv col = if withWrapped (defaultIsSingleLine . vectorProxy) col then toS . toLazyText $ buildSingleLine col
--               else unlinesV $ buildSingleColumn col

-- -- instance ValueShow GroupedCol where
--   -- buildSingleLine = undefined -- withWrapped $ \(Comp v) ->
--     -- buildSingleLineVector (vectorProxy v) $ buildAtomRaw1 <$> v
--   -- showv col = if withWrapped (defaultIsSingleLine . vectorProxy) col then toS . toLazyText $ buildSingleLine col
--               -- else unlinesV $ buildSingleColumn col

-- vectorProxy :: Vector (g a) -> Proxy a
-- vectorProxy _ = Proxy

-- instance AtomShow a => ValueShow (Vector a) where
--   buildSingleLine = buildSingleLineVector (Proxy @a) . fmap buildAtomRaw
--   {-# INLINE buildSingleLine #-}
--   showv = if defaultIsSingleLine (Proxy @a) then toS . toLazyText . buildSingleLine
--     else unlinesV . buildSingleColumn
--   {-# INLINABLE showv #-}

-- instance AtomShow a => DictComponentShow (Vector a) where
--   getDictBuilder = singleColumnInDict Nothing
--   {-# INLINABLE getDictBuilder #-}

-- instance AtomShow a => SingleColumnShow (Vector a) where
--   buildSingleColumn = fmap buildAtomInColumn
--   {-# INLINE buildSingleColumn #-}

-- instance (DictComponentShow a, DictComponentShow b) => ValueShow (Dict a b) where
--   buildSingleLine d = buildSingleLine (key d) <> singleton '!' <> buildSingleLine (value d) 
--   {-# INLINABLE buildSingleLine #-}
--   showv d = unlinesV $ combine $ horizontalConcat "| " $ trans <$> (keys :| [vals])
--     where (keys, vals) = (getDictBuilder $ key d, getDictBuilder $ value d)
--           trans x = InDictBuilder 0 "" False $ combine $ x { hNonEmtpyHeader = on (||) hNonEmtpyHeader keys vals }
--   {-# INLINABLE showv #-}

-- -- instance DictComponentShow GroupedTableRaw where
--   -- getDictBuilder t      = maybe (throw $ TableWithNoColumn "cannot show") (horizontalConcat " ")
--     -- $ nonEmpty $ V.toList $ zipTable (singleColumnInDict . Just) t
  
-- instance DictComponentShow Table where
--   getDictBuilder t      = maybe (throw $ TableWithNoColumn "cannot show") (horizontalConcat " ")
--     $ nonEmpty $ V.toList $ zipTable (singleColumnInDict . Just) t
  
-- -- | I do not think this is needed or used in Q
-- -- instance SingleColumnShow Table where
--   -- buildSingleColumn t = V.generate (count t) $ buildSingleLine . unsafeRowDict t

-- -- instance ValueShow GroupedTableRaw where
--   -- buildSingleLine t = "+" <> buildSingleLine (flipTable t) 
--   -- showv = unlinesV . combine . getDictBuilder

-- instance ValueShow Table where
--   buildSingleLine t = "+" <> buildSingleLine (flipTable t) 
--   showv = unlinesV . combine . getDictBuilder


-- instance Show Table where show = toS . showv
-- instance Show TableCol where show = toS . showv
-- instance Show TableCell where show = toS . showSingleton
-- instance (DictComponentShow a, DictComponentShow b) => Show (Dict a b) where show = toS . showv

-- showSingleton :: forall a . AtomShow a => a -> Text
-- showSingleton = toS . toLazyText . buildSingleton (Proxy @a) . buildAtomRaw
-- {-# INLINE showSingleton #-}

-- -- * zip fold map

-- zipTable :: (Symbol -> TableCol' f -> a) -> Table' f -> Vector a
-- zipTable f t = V.zipWith f (cols t) $ vals t
-- {-# INLINABLE zipTable #-}

-- zipTable3 :: (Symbol -> TableCol' f -> c -> a) -> Table' f -> Vector c -> Vector a
-- zipTable3 f t = V.zipWith3 f (cols t) $ vals t
-- {-# INLINABLE zipTable3 #-}

-- mapTableWithName :: (Symbol -> TableCol' f -> TableCol' g) -> Table' f -> Table' g
-- mapTableWithName f = unsafeTable_ %~ mapDictWithKey f

-- mapTable :: (TableCol' f -> TableCol' g) -> Table' f -> Table' g
-- mapTable f = unsafeTable_ %~ mapDictValue (fmap f)

-- mapTableWrapped :: (forall g a. (Wrappable2 g a) => f (g a) -> h (g a)) -> Table' f -> Table' h
-- mapTableWrapped f = mapTable $ mapWrapped f


-- -- * operations

-- rename :: [Symbol] -> [Symbol] -> Table -> Table
-- rename oldNames newNames = unsafeTable_ %~ mapDictKey (fmap $ \on -> fromMaybe on $ HM.lookup on map)
--   where map = HM.fromList $ zip oldNames newNames


-- -- | first steps towards a grouped table (this is experimental)
-- xgroup :: Symbols -> Table -> (Table, GroupedTableRaw)
-- xgroup s table = (keys,) $ mapTableWrapped (Comp . applyGrouping idxs) valueInput
--   where (keys, idxs) = getGroupsAndGrouping keyInput
--         (keyInput, valueInput)  = UnsafeTable *** UnsafeTable $ intersectAndDiff (toVector s) $ flipTable table

-- keyed :: (ToTable k, ToTable v) => (k, v) -> KeyedTable
-- keyed (k,v) = dict (toTable k) $ toTable v

-- fromKeyed :: (FromTable k, FromTable v) => KeyedTable -> (k, v)
-- fromKeyed = fromTable . key &&& fromTable . value


-- xkey :: HasCallStack => [Symbol] -> Table -> KeyedTable
-- xkey s = uncurry dictNoLengthCheck . (UnsafeTable *** UnsafeTable) . intersectAndDiff (toVector s) . flipTable

-- unkey :: KeyedTable -> Table
-- unkey kt =  UnsafeTable $ on (<>) flipTable (key kt) $ value kt

-- unkeyI :: Wrappable a => Symbol -> SingleKeyTable a -> Table
-- unkeyI keyName kt =  UnsafeTable $ on (<>) flipTable (keyName <# key kt) $ value kt

-- unkeyF :: (HasCallStack, Wrappable1 f, Wrappable a) => Symbol -> SingleKeyTable (f a) -> Table
-- unkeyF keyName kt =  UnsafeTable $ on (<>) flipTable (keyName `tcF` key kt) $ value kt


-- meta :: (ToTable t, HasCallStack) => t -> Table
-- meta t' = table [("c", toWrappedDynI $ cols t), ("t", toWrappedDynI $ withWrapped' (\tr _ -> show tr) <$> vals t)]
--   where t = toTable t'

-- -- fillCol :: forall a . Wrappable a => a -> TableCol ->  TableCol
-- -- fillCol val x@(WrappedDyn t col)
-- --   | Just HRefl <- t `eqTypeRep` expected                = x
-- --   | App con v <- t
-- --   , Just HRefl <- v `eqTypeRep` expected
-- --   , Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = toWrappedDyn $ fromMaybe val <$> col
-- --   | True                                                = throw $ TypeMismatch $ "Expected (Maybe) " <> show expected <> " got " <> show t
-- --   where expected = typeRep :: TypeRep a



-- catMaybes' :: Typeable a => TableCol -> Vector a
-- catMaybes' = \case
--   c@(WrappedDyn tr@(App con _) v)
--     | Just HRefl <- tr    `eqTypeRep` typeRep @(I None)  -> mempty
--     | Just HRefl <- con   `eqTypeRep` typeRep @I         -> fromWrappedDyn c
--     | Just HRefl <- con   `eqTypeRep` typeRep @Maybe            -> fromWrappedDyn $ toWrappedDynI $ V.catMaybes v
--   w             -> errorMaybeOrI "n/a" w

-- catMaybes_ :: TableCol -> TableCol
-- catMaybes_ = \case
--   c@(WrappedDyn tr@(App con _) v)
--     | Just HRefl <- tr    `eqTypeRep` typeRep @(I None)  -> throw $ TypeMismatch "catMaybes not supported for I None"
--     | Just HRefl <- con   `eqTypeRep` typeRep @I         -> c
--     | Just HRefl <- con   `eqTypeRep` typeRep @Maybe            -> toWrappedDynI $ V.catMaybes v
--   w             -> errorMaybeOrI "n/a" w

-- withMaybeCol :: HasCallStack => (forall a . Maybe a -> Bool) -> TableCol -> Vector Bool
-- withMaybeCol f = \case
--   (WrappedDyn tr@(App con _) v)
--     | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> False <$ v
--     | Just HRefl <- con   `eqTypeRep` typeRep @I        -> True  <$ v
--     | Just HRefl <- con   `eqTypeRep` typeRep @Maybe        -> f <$> v
--   w             -> errorMaybeOrI "n/a" w

-- isRightCol :: HasCallStack => TableCol -> Vector Bool
-- isRightCol = \case
--   (WrappedDyn (App (App con _) _) v)
--     | Just HRefl <- con `eqTypeRep` R.typeRep @Either   -> isRight <$> v
--   (WrappedDyn tr _)                                     -> throw $ TypeMismatch $ "Expected I Either got " <> show tr

-- fromRightColUnsafe :: HasCallStack => TableCol -> TableCol
-- fromRightColUnsafe = \case
--   (WrappedDyn (App (App con _) _) v)
--     | Just HRefl <- con `eqTypeRep` R.typeRep @Either   -> toWrappedDynI $ fromRight Prelude.undefined <$> v
--   (WrappedDyn tr _)                                     -> throw $ TypeMismatch $ "Expected I Either got " <> show tr

-- fromLeftColUnsafe :: forall l . HasCallStack => Wrappable l => Proxy l -> TableCol -> TableCol
-- fromLeftColUnsafe _ = \case
--   (WrappedDyn (App (App con l) _) v)
--     | Just HRefl <- con `eqTypeRep` R.typeRep @Either   
--     , Just HRefl <- l `eqTypeRep` R.typeRep @l          -> toWrappedDynI $ fromLeft Prelude.undefined <$> v
--   (WrappedDyn tr _)      -> throw $ TypeMismatch $ "Expected 'Either " <> show (R.typeRep @l) <> "' got " <> show tr

-- fromJustCol :: HasCallStack => Table -> Symbol -> TableCol ->  TableCol
-- fromJustCol table colName col'@(WrappedDyn t@(App con _) col)
--   | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = let res = V.catMaybes col
--     in if length res == length col then toWrappedDynI res
--     else err $ "\n" <> P.take 2000 (show $ unsafeBackpermute (IterIndex <$> V.findIndices isNothing col) table)
--   | Just HRefl <- t `eqTypeRep` R.typeRep @(I None)      = err ": whole column is None"
--   | Just HRefl <- con `eqTypeRep` R.typeRep @I      = col'
--   where err msg = throw $ UnexpectedNulls $ "in Column " <> toS colName <> msg
--         err :: String -> a
-- fromJustCol _     name       w            = errorMaybeOrI name w

-- errorMaybeOrI :: HasCallStack => Text -> WrappedDyn f -> p2
-- errorMaybeOrI name (WrappedDyn t _  ) = throw $ TypeMismatch $ "in Column " <> toS name <>
--   ": Expected Maybe or I got " <> show t

-- -- | convert column to Maybe (if it is not already)
-- toMaybe :: HasCallStack => Symbol -> TableCol ->  TableCol
-- toMaybe = modifyMaybeCol Nothing

-- data TableColModifierWithDefault = TableColModifierWithDefault (forall g . g -> Vector g -> Vector g)

-- modifyMaybeCol :: HasCallStack => Maybe TableColModifierWithDefault -> Symbol -> TableCol -> TableCol
-- modifyMaybeCol fM _ col'@(WrappedDyn t@(App con _) col)
--   | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = g Nothing col' col
--   | Just HRefl <- t   `eqTypeRep` R.typeRep @(I None) = g (I $ None ()) col' col
--   | Just HRefl <- con `eqTypeRep` R.typeRep @I   = let c = Just <$> coerceUI col in g Nothing (toWrappedDyn c) c
--   where g :: forall g a . Wrappable2 g a => g a -> TableCol -> Vector (g a) -> TableCol
--         g v wrapped raw = case fM of
--           Just (TableColModifierWithDefault f)      -> toWrappedDyn $ f v raw
--           _                                             -> wrapped
-- modifyMaybeCol  _ name       w            = errorMaybeOrI name w
                               

-- unsafeBackpermuteMaybeTableCol :: HasCallStack => Vector (Maybe IterIndex) -> Symbol -> TableCol -> TableCol
-- unsafeBackpermuteMaybeTableCol ks _ (WrappedDyn t@(App con _) col)
--   | Just HRefl <- t `eqTypeRep` R.typeRep @(I None) = toWrappedDynI $ V.replicate (count col) $ None ()
--   | Just HRefl <- con `eqTypeRep` R.typeRep @I =
--       toWrappedDyn $ unsafeBackpermuteMaybeV (coerceUI col) (coerce ks)
--   | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe    =
--       toWrappedDyn $ fmap join $ unsafeBackpermuteMaybeV col (coerce ks)
-- unsafeBackpermuteMaybeTableCol _ name       w            = errorMaybeOrI name w

-- -- * joins

-- -- | helper for joins
-- combineCols :: HasCallStack => 
--   (forall g a . Wrappable2 g a => Vector (g a) -> Vector (g a) -> TableCol) -> Symbol -> TableCol -> TableCol -> TableCol
-- combineCols combineVec name (WrappedDyn t1@(App con1 v1) col1) (WrappedDyn t2@(App con2 v2) col2) = case v1 `eqTypeRep` v2 of
--   Just HRefl | Just HRefl <- con1 `eqTypeRep` R.typeRep @I -> case () of
--                  () | Just HRefl <- con2 `eqTypeRep` R.typeRep @I -> combineVec col1 col2
--                     | Just HRefl <- con2 `eqTypeRep` R.typeRep @Maybe    -> combineVec (Just <$> coerceUI col1) col2
--                  _ -> err
--              | Just HRefl <- con1 `eqTypeRep` R.typeRep @Maybe -> case () of
--                  () | Just HRefl <- con2 `eqTypeRep` R.typeRep @Maybe    -> combineVec col1 col2
--                     | Just HRefl <- con2 `eqTypeRep` R.typeRep @I -> combineVec col1 $ Just <$> coerceUI col2
--                  _ -> err
--   _  -> err
--   where err = tableJoinError name t1 t2
-- combineCols _ name (WrappedDyn t1 _  ) (WrappedDyn t2 _  ) = tableJoinError name t1 t2
-- {-# INLINABLE combineCols #-}

-- tableJoinError :: HasCallStack => (Show a2, Show a3) => Symbol -> a3 -> a2 -> a
-- tableJoinError name t1 t2 = throw $ TypeMismatch
--   $ "Column " <> toS name <> ": trying to join " <> show t2 <> " onto " <> show t1
-- {-# INLINABLE tableJoinError #-}


-- -- | inner join
-- ij :: HasCallStack => Table -> KeyedTable -> Table
-- ij t kt = mergeTablesPreferSecond (unsafeBackpermute idxsLeft t) $ unsafeBackpermute idxs $ value kt
--   where (idxsLeft, idxs) = V.unzip $ safeMapAccess (\hm -> V.imapMaybe (\i -> fmap (IterIndex i,) . (hm HM.!?))) t kt

-- -- | inner join (cartesian)
-- ijc :: HasCallStack => Table -> KeyedTable -> Table
-- ijc t kt = mergeTablesPreferSecond (unsafeBackpermute idxsLeft t) $ unsafeBackpermute idxsRight $ value kt
--   where (idxsGroupLeft, idxsGroupRight) = V.unzip $ safeMapAccess (\hm -> V.imapMaybe (\i -> fmap (IterIndex i,) . (hm HM.!?))) t groupsToIndices
--         groupsToIndices = unsafeGroupingDict kt
--         (idxsRight, idxsLeft) = groupingToConcatenatedIndicesAndDuplication (value groupsToIndices) idxsGroupRight idxsGroupLeft

-- groupingToConcatenatedIndicesAndDuplication :: Grouping -> Vector IterIndex -> Vector a -> (Vector IterIndex, Vector a)
-- groupingToConcatenatedIndicesAndDuplication grp grpIdxs vec =
--   (V.concat $ toList permutedGroupIdxs
--   , V.concat $ V.toList $ V.zipWith (V.replicate . V.length) permutedGroupIdxs vec)
--   where permutedGroupIdxs = unsafeBackpermute grpIdxs grp


-- -- | left join
-- lj :: HasCallStack => Table -> KeyedTable -> Table
-- lj t kt | all isJust idxs       = mergeTablesWith (\_ a -> a) toMaybe (combineCols $ \_ b -> toWrappedDyn b)
--                                   t $ unsafeBackpermute (Data.Maybe.fromJust <$> idxs) $ value kt
--         | True                  = mergeTablesWith (\_ a -> a)
--           (\name a -> unsafeBackpermuteMaybeTableCol idxs name a) fill t $ value kt
--   where idxs = safeMapAccess (\hm -> fmap (hm HM.!?)) t kt
--         fill  = combineCols $ \col1 col2 -> toWrappedDyn $ V.zipWith (\old new -> fromMaybe old new) col1
--                                             $ unsafeBackpermuteMaybeV col2 $ coerce idxs

-- -- | left join (cartesian)
-- ljc :: HasCallStack => Table -> KeyedTable -> Table

-- ljc t kt | all isJust idxsGroupRight = mergeTablesWith inputPermute toMaybe (combineCols $ \_ b -> toWrappedDyn b) t
--                                        $ unsafeBackpermute (Data.Maybe.fromJust <$> idxsRight) $ value kt
--          | True                  = mergeTablesWith inputPermute
--                                    (unsafeBackpermuteMaybeTableCol idxsRight) fill t $ value kt
--   where idxsGroupRight = safeMapAccess (\hm -> fmap (hm HM.!?)) t groupsToIndices
--         fill  = combineCols $ \col1 col2 -> toWrappedDyn $ unsafeBackpermuteMaybe2 col1 col2 (coerce idxsLeft) $ coerce idxsRight
--         groupsToIndices = unsafeGroupingDict kt
--         (idxsRight, idxsLeft) = groupingToConcatenatedIndicesAndDuplicationM (value groupsToIndices) idxsGroupRight $
--                                 V.enumFromN 0 (count t)
--         inputPermute _ = mapWrapped $ unsafeBackpermute idxsLeft


-- groupingToConcatenatedIndicesAndDuplicationM :: Grouping -> Vector (Maybe IterIndex) -> Vector a -> (Vector (Maybe IterIndex), Vector a)
-- groupingToConcatenatedIndicesAndDuplicationM grp grpIdxs vec =
--   (V.concat $ toList $ maybe (V.singleton Nothing) (fmap Just) <$> permutedGroupIdxs
--   , V.concat $ V.toList $ V.zipWith (maybe V.singleton (V.replicate . V.length)) permutedGroupIdxs vec)
--   where permutedGroupIdxs = unsafeBackpermuteMaybe grpIdxs grp


-- -- | prepend (first arg) and append (second arg) Nothing's to a column vector (turning it to Maybe as needed)
-- padColumn :: HasCallStack => Int -> Int -> Symbol -> TableCol -> TableCol
-- padColumn prependN appendN = modifyMaybeCol $ Just $ TableColModifierWithDefault $ \def -> 
--   let  prepend | prependN <= 0 = id
--                | True          = (V.replicate prependN def <>)
--        append  | appendN <= 0  = id
--                | True          = (<> V.replicate appendN def)
--   in prepend . append

-- -- | union join table
-- ujt :: HasCallStack => Table -> Table -> Table
-- ujt t1 t2 = mergeTablesWith (padColumn 0 $ count t2) (padColumn (count t1) 0)
--   (combineCols $ \a b -> toWrappedDyn $ a <> b) t1 t2


-- joinMatchingTables :: HasCallStack => Table -> Table -> Table
-- joinMatchingTables t1 t2 = orderedColumnIntersection True (\v1 v2 _ -> toWrappedDyn $ v1 <> v2) t1 t2

-- -- | union join keyed table
-- ujk :: HasCallStack => KeyedTable -> KeyedTable -> KeyedTable
-- ujk kt1 kt2 = dictNoLengthCheck combinedUniqueKeys $ mergeTablesWith
--               (unsafeBackpermuteMaybeTableCol idxs1) (unsafeBackpermuteMaybeTableCol idxs2) fill (value kt1) $ value kt2
--   where (idxs1, idxs2) = fmap (unsafeMap kt1 HM.!?) &&& fmap (unsafeMap kt2 HM.!?) $ toUnsafeKeyVector $ combinedUniqueKeys
--         combinedUniqueKeys = distinct $ on joinMatchingTables key kt1 kt2
--         fill = combineCols $ \v1 v2 -> toWrappedDyn $ Hoff.Vector.unsafeBackpermuteMaybe2 v1 v2 (coerce $ Data.Maybe.fromJust <$> idxs1)
--           $ coerce idxs2

-- -- -- | union join keyed table (cartesian)
-- -- ujc :: HasCallStack => KeyedTable -> KeyedTable -> KeyedTable

-- -- | turns the given columns to I and the other columns to Maybe
-- -- throws TypeMismatch and UnexpectedNulls
-- fromJusts :: (HasCallStack, ToVector f) => f Symbol -> Table -> Table
-- fromJusts columns table | null errs = flip mapTableWithName table $ \col v ->
--                             if V.elem col cvec then fromJustCol table col v else toMaybe col v
--                         | True      = throw $ KeyNotFound $ ": The following columns do not exist:\n" <> show errs <> "\nAvailable:\n"
--                                       <> show (cols table)
--   where errs = missing cvec $ flipTable table
--         cvec = toVector columns

-- allToMaybe :: HasCallStack => Table -> Table
-- allToMaybe  = mapTableWithName toMaybe

-- allFromJusts :: HasCallStack => Table -> Table
-- allFromJusts t = fromJusts (cols t) t

-- mergeTablesWith :: HasCallStack => (Symbol -> TableCol -> TableCol) -> (Symbol -> TableCol -> TableCol)
--                 -> (Symbol -> TableCol -> TableCol -> TableCol) -> Table -> Table -> Table
-- mergeTablesWith a b f t1 t2 = table2 $ on (fillDictWith a b f) flipTable t1 t2

-- mergeTablesPreferSecond :: HasCallStack => Table -> Table -> Table
-- mergeTablesPreferSecond = mergeTablesWith (\_ a -> a) (\_ a -> a) (\_ _ f -> f)

-- -- | mergeTablesPreferFirst
-- fillTable :: HasCallStack => Table -> Table -> Table
-- fillTable = mergeTablesWith (\_ a -> a) (\_ a -> a) (\_ f _ -> f)

-- type TableRow r = (AllFields r Typeable, KnownFields r)

-- -- * convert to statically typed
-- rows :: (TableRow r) => Table -> Vector (Record I r)
-- rows = flipRecord . columns

-- instance TableRow r => FromTable (TableR r) where
--   fromTable t = R.cmap (Proxy @Typeable) (\(K name) -> fromWrappedDynF $ f name) $ R.reifyKnownFields Proxy
--     where f = (flipTable t !) . toS
--   {-# INLINABLE fromTable #-} 
  
-- instance (AllFields r Wrappable3, TableRow r) => ToTable (TableR r) where
--   toTable = table . fmap (first toS) . R.toList . R.cmap (Proxy @Wrappable3)
--             (K . toWrappedDyn)
--   {-# INLINABLE toTable #-}

-- -- | special treatment for I
-- columns :: (HasCallStack, TableRow r) => Table -> TableR r
-- columns t = R.cmap (Proxy @Typeable) (\(K name) -> fromWrappedDyn $ f name) $ R.reifyKnownFields Proxy
--   where f = (flipTable t !) . toS
-- {-# INLINABLE columns #-}

-- -- | special treatment for I
-- fromColumnsI :: (AllFields r Wrappable, HasCallStack, TableRow r) => TableR r -> Table
-- fromColumnsI = table . fmap (first toS) . R.toList . R.cmap (Proxy @Wrappable)
--               (K . toWrappedDynI)
-- {-# INLINABLE fromColumnsI #-}
  
-- flipRecord :: (KnownFields r, HasCallStack) => TableR r -> Vector (Record I r)
-- flipRecord cs = case equalLength $ uncurry dictNoLengthCheck $ V.unzip
--   $ V.fromList $ fmap (first (toS::String -> Symbol)) $ R.toList $ R.map (K . V.length) cs of
--   Nothing               -> mempty
--   Just (n, Nothing)     -> V.generate n $ \idx -> R.map (\v -> I $ V.unsafeIndex v idx) cs
--   Just (_, Just err)    -> throw $ TableDifferentColumnLenghts err
-- {-# INLINABLE flipRecord #-}

-- -- zipRecord :: (a -> b -> x) -> TableR '["a" := a, "b" := b] -> Vector x
-- -- zipRecord f r = V.zipWith f (R.get #a r) (R.get #b r)

-- -- zipRecord3 :: (a -> b -> c -> x) -> TableR '["a" := a, "b" := b, "c" := c] -> Vector x
-- -- zipRecord3 f r = V.zipWith3 f (R.get #a r) (R.get #b r) (R.get #c r)

-- -- zipRecord4 :: (a -> b -> c -> d -> x) -> TableR '["a" := a, "b" := b, "c" := c, "d" := d] -> Vector x
-- -- zipRecord4 f r = V.zipWith4 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r)

-- -- zipRecord5 :: (a -> b -> c -> d -> e -> x) -> TableR '["a" := a, "b" := b, "c" := c, "d" := d, "e" := e] -> Vector x
-- -- zipRecord5 f r = V.zipWith5 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r) (R.get #e r)

-- -- zipRecord6 :: (a -> b -> c -> d -> e -> f -> x) -> TableR '["a" := a, "b" := b, "c" := c, "d" := d, "e" := e, "f" := f] -> Vector x
-- -- zipRecord6 f r = V.zipWith6 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r) (R.get #e r) (R.get #f r)

-- -- zipTable :: (Typeable a, Typeable b) => (a -> b -> x) -> Table -> Vector x
-- -- zipTable f = zipRecord f . columns

-- -- zipTable3 :: (Typeable a, Typeable b, Typeable c) => (a -> b -> c -> x) -> Table -> Vector x
-- -- zipTable3 f = zipRecord3 f . columns

-- -- zipTable4 :: (Typeable a, Typeable b, Typeable c, Typeable d) => (a -> b -> c -> d -> x) -> Table -> Vector x
-- -- zipTable4 f = zipRecord4 f . columns

-- -- zipTable5 :: (Typeable a, Typeable b, Typeable c, Typeable d, Typeable e) => (a -> b -> c -> d -> e -> x) -> Table -> Vector x
-- -- zipTable5 f = zipRecord5 f . columns

-- -- zipTable6 f = zipRecord6 f . columns


-- summaryKeys :: Vector Text
-- summaryKeys = toVector ["type"
--                        ,"non nulls"
--                        ,"nulls"
--                        ,"unique"
--                        ,"min"
--                        ,"max"
--                        ,"most frequent"
--                        ,"frequency"]

-- summary :: ToTable t => t -> KeyedTable
-- summary = dict ("s" <# summaryKeys) . mapTableWithName summaryCol . toTable

-- summaryCol :: Symbol -> TableCol -> TableCol
-- summaryCol name = \case
--   c@(WrappedDyn tr@(App con _) v)
--     | Just HRefl <- tr    `eqTypeRep` typeRep @(I None)  -> summaryVec c $ V.empty @()
--     | Just HRefl <- con   `eqTypeRep` typeRep @I         -> summaryVec c $ coerceUI v
--     | Just HRefl <- con   `eqTypeRep` typeRep @Maybe            -> summaryVec c $ V.catMaybes v
--   w             -> errorMaybeOrI name w

-- summaryVec :: forall a . Wrappable a => TableCol -> Vector a -> TableCol
-- summaryVec (WrappedDyn tr fullVec) nonnull = toWrappedDynI $ toVector
--   [shot tr
--   ,le nonnull
--   ,nulls
--   ,le $ distinct nonnull
--   ,mm V.minimum
--   ,mm V.maximum
--   ,fst maxFreq
--   ,snd maxFreq]
--   where mm f | null nonnull     = "n/a" :: Text
--              | True             = toText $ f $ nonnull 
--         nulls = toS $ show $ length fullVec - length nonnull
--         maxFreq | null nonnull  = ("n/a", "0")
--                 | True          = toText *** shot $ Data.List.maximumBy (comparing snd) $ HM.toList
--                                   $ HM.fromListWith (+) $ toList $ fmap (,1::Int) nonnull
--         le = shot . length
--         shot = toS . show
--         shot :: Show x => x ->  Text
--         toText :: a -> Text 
--         toText = toS . toLazyText . buildAtomInColumn
