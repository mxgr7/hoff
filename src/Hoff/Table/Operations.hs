{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Hoff.Table.Operations where

import           Control.Lens
import           Data.Coerce
import qualified Data.HashMap.Strict as HM
import qualified Data.List
import qualified Data.Maybe
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Record.Anon.Advanced as R
import           Data.SOP
import           Data.String
import           Data.Type.Equality
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.Show
import           Hoff.Table.Show ()
import           Hoff.Table.Types
import           Hoff.Utils
import           Hoff.Vector
import qualified Prelude
import           TextShow as B hiding (fromString)
import           Type.Reflection as R
import           Yahp as P hiding (get, group, groupBy, (&), TypeRep, typeRep, Hashable, hashWithSalt, (:.:), null, (<|>))

instance ToTableAndBack Table where
  type InH Table = Table
  unsafeBack _ = id
  {-# INLINE unsafeBack #-}

instance ToTableAndBack TableH where
  type InH TableH = Table
  unsafeBack _ = id
  {-# INLINE unsafeBack #-}

instance ToTableAndBack KeyedTableH where
  type InH KeyedTableH = KeyedTable
  unsafeBack kt t = bind kt $ flip xkey t . toList . cols . key
  {-# INLINABLE unsafeBack #-}

instance ToTableAndBack KeyedTable where
  type InH KeyedTable = KeyedTable
  unsafeBack kt = unsafeBack $ pure @H kt
  {-# INLINE unsafeBack #-}

useToTableAndBack :: (HasCallStack, ToTableAndBack t) => (TableH -> TableH) -> t -> H' t
useToTableAndBack f t' = let (t, back) = toTableAndBack t' in back $ f t
{-# INLINE useToTableAndBack #-}

instance ToH Table KeyedTable               where toH = unkey
instance ToH Table KeyedTableH              where toH = unkey

instance DistinctIterable Table where
  distinct t | count (cols t) == 1 = mapTable (mapTableCol distinctV) t
             | True                = unsafeBackpermute (coerce $ distinctVIndices $ toUnsafeKeyVector t) t
  {-# INLINABLE distinct #-}
             

-- instance Iterable (Table' f) => DictComponent (Table' f) where
--   type KeyItem (Table' f)= TableKey' f
--   type Nullable (Table' f) = (Table' f)

instance Iterable Table => DictComponent Table where
  type KeyItem Table= TableKey
  type Nullable Table = Table



  -- {-# INLINE (!) #-}
  -- (!)             :: HasCallStack => Table          -> Int  -> TableKey
  -- t ! idx = A.map (\col -> I $ col V.! idx) t 

  unsafeBackpermuteMaybe = mapMTableWithName . unsafeBackpermuteMaybeTableCol
  {-# INLINE unsafeBackpermuteMaybe #-}


  toUnsafeKeyVector t = V.generate (count t) (UnsafeTableKey t . IterIndex)
  {-# INLINABLE toUnsafeKeyVector #-}

  toCompatibleKeyVector that this = toUnsafeKeyVector <$> orderedColumnIntersection False keepSecond that this
  {-# INLINABLE toCompatibleKeyVector #-}
          

-- | returns a subset of columns of the second table with column order and types as the first
-- the columns resulting columns are combined with the columns of the first table using the given combination function
orderedColumnIntersection :: HasCallStack => Bool ->
  (forall g a . Wrappable2 g a => Vector (g a) -> Vector (g a) -> TableCol -> TableCol)
  -> Table -> Table -> TableH
orderedColumnIntersection requireIdenticalColumnSet combine sourceTable targetTable
  | Prelude.null allErrs        = table2 =<< dict requiredCols
                                  =<< zipTable3M combine2 sourceTable subTargetVals
  | True                        = throwH $ IncompatibleTables $ Prelude.unlines $ allErrs
  where (subTargetVals, additionalCols) = first value $ intersectAndDiff requiredCols targetCols
        combine2 name (WrappedDyn t1 v1) w2@(WrappedDyn t2 v2)
          | Just HRefl <- t1 `eqTypeRep` t2   = pure $ combine v1 v2 w2
          | True                              = throwH $ TypeMismatch
            $ "Column " <> toS name <> ": " <> show t1 <> " /=  " <> show t2
        requiredCols = cols sourceTable
        targetCols = flipTable targetTable
        allErrs = catMaybes
          [("Missing from target table: " <> show (missing requiredCols targetCols))
           <$ guard (count requiredCols /= count subTargetVals)
          ,("Missing from source table: " <> show (key additionalCols))
           <$ guard (requireIdenticalColumnSet && not (null additionalCols))
          ]
{-# INLINABLE orderedColumnIntersection #-}
-- * from/to WrappedDyns

-- | convenience: accepts (f a) instead of (f (I a))
toWrappedDynI :: (ICoerce f a, Wrappable a) => f a -> WrappedDyn f
toWrappedDynI = WrappedDyn typeRep . coerceI
{-# INLINABLE toWrappedDynI #-}

-- | convenience: unwraps (I a) to a
fromWrappedDyn :: forall a f . (ICoerce f a, Functor f, Typeable a, HasCallStack) => Maybe Symbol -> WrappedDyn f -> H (f a)
fromWrappedDyn _ (WrappedDyn (App con t) v)
  | Just HRefl <- con   `eqTypeRep` typeRep @I
  , Just HRefl <- t     `eqTypeRep` typeRep @a                  = pure $ coerceUI v
fromWrappedDyn n w = fromWrappedDynF n w
{-# INLINABLE fromWrappedDyn #-}

-- | the full blown version (which has an inverse: toWrappedDyn (up to I None ~ Maybe a equivalence))
fromWrappedDynF :: forall a f . (ICoerce f a, Functor f, Typeable a, HasCallStack) => Maybe Symbol -> WrappedDyn f -> H (f a)
fromWrappedDynF _ (WrappedDyn tf v)
  | Just HRefl <- ta    `eqTypeRep` tf                           = pure $ v
  | Just HRefl <- tf    `eqTypeRep` typeRep @(I None)
  , App cona _ <- ta
  , Just HRefl <- cona `eqTypeRep` typeRep @Maybe               = pure $ Nothing <$ v
  where ta  = typeRep @a
fromWrappedDynF name (WrappedDyn t _ ) = throwH $ TypeMismatch $
  "Column " <> maybe "<unknown>" show name <> ": Cannot convert " <> show t <> " to " <> show (typeRep @a)
{-# INLINABLE fromWrappedDynF #-}

-- * operations

mergeTablesWith :: (HasCallStack, ToTable a, ToTable b) => (Symbol -> TableCol -> TableColH) -> (Symbol -> TableCol -> TableColH)
                -> (Symbol -> TableCol -> TableCol -> TableColH) -> a -> b -> TableH
mergeTablesWith a b f = liftToH2 $ \t1 t2 -> table2 =<< on (fillDictWithM a b f) flipTable t1 t2
{-# INLINABLE mergeTablesWith #-}

mergeTablesPreferSecond :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
mergeTablesPreferSecond = mergeTablesWith untouchedA untouchedA keepSecondA
{-# INLINABLE mergeTablesPreferSecond #-}

mergeTablesPreferFirst :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
mergeTablesPreferFirst = mergeTablesWith untouchedA untouchedA keepFirstA
{-# INLINABLE mergeTablesPreferFirst #-}


(//) :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
(//) = mergeTablesPreferSecond
{-# INLINE (//) #-}

(\\) :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
(\\) = mergeTablesPreferFirst
{-# INLINE (\\) #-}

(/|) :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
(/|) = liftToH2 $ fmap2 table2 $ on (<>) flipTable
{-# INLINE (/|) #-}


infixr 4 //
infixr 4 /|
infixr 4 \\

ensureCol :: (ToTable t, Wrappable a, Wrappable1 f) => Symbol -> f a -> t -> TableH
ensureCol n v = chainToH @Table $ \t -> t \\ tcF n (V.replicate (count t) v)
{-# INLINABLE ensureCol #-}


rename :: ToTable t => [Symbol] -> [Symbol] -> t -> TableH
rename oldNames newNames = mapToH $ unsafeTableWithColumns_ %~ mapDictKeys (\on -> fromMaybe on $ HM.lookup on map)
  where map = HM.fromList $ zip oldNames newNames
{-# INLINABLE rename #-}


-- | first steps towards a grouped table (this is experimental)
xgroup :: ToTable t => Symbols -> t -> H (Table, GroupedTableRaw)
xgroup s = chainToH $ \table -> do
  let (keyInput, valueInput)  = intersectAndDiff (toVector s) $ flipTable table
  (keys, idxs) <- getGroupsAndGrouping <$> tableNoLengthCheck keyInput
  (keys,) . mapTableWrapped (Comp . applyGrouping idxs) <$> tableNoLengthCheck valueInput
{-# INLINABLE xgroup #-}


key' :: ToKeyedTable t => t -> H Table
key' = mapToH @KeyedTable key
{-# INLINE key' #-}

value' :: ToKeyedTable t => t -> H Table
value' = mapToH @KeyedTable value
{-# INLINE value' #-}

-- ** ToH combinators

mapToH :: ToH v t => (v -> a) -> t -> H a
mapToH f = fmap f . toH
{-# INLINE mapToH #-}

chainToH :: ToH v t => (v -> H a) -> t -> H a
chainToH f = chain f . toH
{-# INLINE chainToH #-}

liftToH2 :: forall va vb a b c . (ToH va a, ToH vb b) => (va -> vb -> H c) -> a -> b -> H c
liftToH2 f x y = do { a <- toH x; b <- toH y; f a b }
{-# INLINABLE liftToH2 #-}

xkey :: (ToTable t, HasCallStack) => [Symbol] -> t -> KeyedTableH
xkey s = chainToH $ \t -> let (k,v) = intersectAndDiff (toVector s) $ flipTable t
                       in dictNoLengthCheck <$> tableNoLengthCheck k <*> tableNoLengthCheck v
{-# INLINABLE xkey #-}
  

unkey :: ToKeyedTable kt => kt -> TableH
unkey = mapToH unkey'
{-# INLINABLE unkey #-}

unkey' :: KeyedTable -> Table
unkey' kt = UnsafeTableWithColumns $ on (<>) flipTable (key kt) $ value kt

unkeyI :: Wrappable a => Symbol -> SingleKeyTable a -> Table
unkeyI keyName kt = UnsafeTableWithColumns $ on (<>) flipTable (tc' keyName $ key kt) $ value kt
{-# INLINABLE unkeyI #-}

unkeyF :: (HasCallStack, Wrappable1 f, Wrappable a) => Symbol -> SingleKeyTable (f a) -> Table
unkeyF keyName kt =  UnsafeTableWithColumns $ on (<>) flipTable (tcF' keyName $ key kt) $ value kt
{-# INLINABLE unkeyF #-}

count' :: ToTable t => t -> H Int
count' = mapToH @Table count
{-# INLINE count' #-}

null' :: ToTable t => t -> H Bool
null' = mapToH @Table null
{-# INLINE null' #-}

take' :: ToTable t => Int -> t -> H Table
take' = mapToH . H.take
{-# INLINE take' #-}

drop' :: ToTable t => Int -> t -> H Table
drop' = mapToH . H.drop
{-# INLINE drop' #-}

distinct' :: ToTable t => t -> H Table
distinct' = mapToH distinct
{-# INLINE distinct' #-}

meta :: (ToTable t, HasCallStack) => t -> TableH
meta t' = do t <- toTable t'
             table [("c", toWrappedDynI $ cols t), ("t", toWrappedDynI $ withWrapped' (\tr _ -> show tr) <$> vals t)]
{-# INLINABLE meta #-}

-- fillCol :: forall a . Wrappable a => a -> TableCol ->  TableCol
-- fillCol val x@(WrappedDyn t col)
--   | Just HRefl <- t `eqTypeRep` expected                = x
--   | App con v <- t
--   , Just HRefl <- v `eqTypeRep` expected
--   , Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = toWrappedDyn $ fromMaybe val <$> col
--   | True                                                = throwH $ TypeMismatch $ "Expected (Maybe) " <> show expected <> " got " <> show t
--   where expected = typeRep :: TypeRep a



catMaybes' :: Typeable a => Maybe Symbol -> TableCol -> VectorH a
{-# INLINABLE catMaybes' #-}
catMaybes' n = \case
  c@(WrappedDyn tr@(App con _) v)
    | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> pure mempty
    | Just HRefl <- con   `eqTypeRep` typeRep @I        -> fromWrappedDyn n c
    | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> fromWrappedDyn n $ toWrappedDynI $ V.catMaybes v
  w             -> errorMaybeOrI n w

catMaybes_ :: Maybe Symbol -> TableCol -> TableColH
catMaybes_ n = \case
  c@(WrappedDyn tr@(App con _) v)
    | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> throwH $ TypeMismatch "catMaybes not supported for I None"
    | Just HRefl <- con   `eqTypeRep` typeRep @I        -> pure c
    | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> pure $ toWrappedDynI $ V.catMaybes v
  w             -> errorMaybeOrI n w
{-# INLINABLE catMaybes_ #-}

isJustCol :: HasCallStack => Maybe Symbol -> TableCol -> VectorH Bool
isJustCol n = \case
  (WrappedDyn tr@(App con _) v)
    | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> pure $ False <$ v
    | Just HRefl <- con   `eqTypeRep` typeRep @I        -> pure $ True  <$ v
    | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> pure $ isJust <$> v
  w             -> errorMaybeOrI n w
{-# INLINABLE isJustCol #-}

isRightCol :: HasCallStack => TableCol -> VectorH Bool
isRightCol = \case
  (WrappedDyn (App (App con _) _) v)
    | Just HRefl <- con `eqTypeRep` R.typeRep @Either   -> pure $ isRight <$> v
  (WrappedDyn tr _)                                     -> throwH $ TypeMismatch $ "Expected I Either got " <> show tr

fromRightColUnsafe :: HasCallStack => TableCol -> TableColH
fromRightColUnsafe = \case
  (WrappedDyn (App (App con _) _) v)
    | Just HRefl <- con `eqTypeRep` R.typeRep @Either   -> pure $ toWrappedDynI $ fromRight Prelude.undefined <$> v
  (WrappedDyn tr _)                                     -> throwH $ TypeMismatch $ "Expected I Either got " <> show tr

fromLeftColUnsafe :: forall l . HasCallStack => Wrappable l => Proxy l -> TableCol -> TableColH
{-# INLINABLE fromLeftColUnsafe #-}
fromLeftColUnsafe _ = \case
  (WrappedDyn (App (App con l) _) v)
    | Just HRefl <- con `eqTypeRep` R.typeRep @Either   
    , Just HRefl <- l `eqTypeRep` R.typeRep @l          -> pure $ toWrappedDynI $ fromLeft Prelude.undefined <$> v
  (WrappedDyn tr _)      -> throwH $ TypeMismatch $ "Expected 'Either " <> show (R.typeRep @l) <> "' got " <> show tr

fromJustCol :: HasCallStack => Maybe Table -> Maybe Symbol -> TableCol ->  TableColH
fromJustCol tableM colNameM col'@(WrappedDyn t@(App con _) col)
  | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = let res = V.catMaybes col
    in if length res == length col then pure $ toWrappedDynI res
    else err $ forMaybe "" tableM $ ("\n" <>) . P.take 2000 . show . unsafeBackpermute (IterIndex <$> V.findIndices isNothing col)
  | Just HRefl <- t `eqTypeRep` R.typeRep @(I None)      = err ": whole column is None"
  | Just HRefl <- con `eqTypeRep` R.typeRep @I      = pure $ col'
  where err msg = throwH $ UnexpectedNulls $ "in Column " <> (maybe "<unknow>" toS colNameM) <> msg
        err :: String -> H a
fromJustCol _     name       w            = errorMaybeOrI name w

errorMaybeOrI :: HasCallStack => Maybe Symbol -> WrappedDyn f -> H a
{-# INLINABLE errorMaybeOrI #-}
errorMaybeOrI name (WrappedDyn t _  ) = throwH $ TypeMismatch $ "in Column " <> maybe "<unknown>" toS name <>
  ": Expected Maybe or I got " <> show t

-- | convert column to Maybe (if it is not already)
toMaybe :: HasCallStack => Symbol -> TableCol ->  TableColH
toMaybe = modifyMaybeCol Nothing

data TableColModifierWithDefault = TableColModifierWithDefault (forall g . g -> Vector g -> Vector g)

modifyMaybeCol :: HasCallStack => Maybe TableColModifierWithDefault -> Symbol -> TableCol -> TableColH
modifyMaybeCol fM _ col'@(WrappedDyn t@(App con _) col)
  | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = g Nothing col' col
  | Just HRefl <- t   `eqTypeRep` R.typeRep @(I None)   = g (I $ None ()) col' col
  | Just HRefl <- con `eqTypeRep` R.typeRep @I          = let c = Just <$> coerceUI col in g Nothing (toWrappedDyn c) c
  where g :: forall g a . Wrappable2 g a => g a -> TableCol -> Vector (g a) -> TableColH
        g v wrapped raw = case fM of
          Just (TableColModifierWithDefault f)  -> pure $ toWrappedDyn $ f v raw
          _                                     -> pure wrapped
modifyMaybeCol  _ name       w            = errorMaybeOrI (Just name) w
                               
unsafeBackpermuteMaybeTableCol :: HasCallStack => Vector (Maybe IterIndex) -> Symbol -> TableCol -> TableColH
unsafeBackpermuteMaybeTableCol ks _ (WrappedDyn t@(App con _) col)
  | Just HRefl <- t `eqTypeRep` R.typeRep @(I None)     = pure $ toWrappedDynI $ V.replicate (count col) $ None ()
  | Just HRefl <- con `eqTypeRep` R.typeRep @I          = pure $ toWrappedDyn $ unsafeBackpermuteMaybeV (coerceUI col) (coerce ks)
  | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = pure $ toWrappedDyn $ fmap join $ unsafeBackpermuteMaybeV col (coerce ks)
unsafeBackpermuteMaybeTableCol _ name       w            = errorMaybeOrI (Just name) w


-- * joins
-- missing: ^ Fill table (todo)

-- | helper for joins
combineCols :: HasCallStack => 
  (forall g a . Wrappable2 g a => Vector (g a) -> Vector (g a) -> TableCol) -> Symbol -> TableCol -> TableCol -> TableColH
combineCols combineVec name (WrappedDyn t1@(App con1 v1) col1) (WrappedDyn t2@(App con2 v2) col2) = case v1 `eqTypeRep` v2 of
  Just HRefl | Just HRefl <- con1 `eqTypeRep` R.typeRep @I -> case () of
                 () | Just HRefl <- con2 `eqTypeRep` R.typeRep @I -> pure $ combineVec col1 col2
                    | Just HRefl <- con2 `eqTypeRep` R.typeRep @Maybe    -> pure $ combineVec (Just <$> coerceUI col1) col2
                 _ -> err
             | Just HRefl <- con1 `eqTypeRep` R.typeRep @Maybe -> case () of
                 () | Just HRefl <- con2 `eqTypeRep` R.typeRep @Maybe    -> pure $ combineVec col1 col2
                    | Just HRefl <- con2 `eqTypeRep` R.typeRep @I -> pure $ combineVec col1 $ Just <$> coerceUI col2
                 _ -> err
  _  -> err
  where err = tableJoinError name t1 t2
combineCols _ name (WrappedDyn t1 _  ) (WrappedDyn t2 _  ) = tableJoinError name t1 t2
{-# INLINABLE combineCols #-}

tableJoinError :: HasCallStack => (Show a2, Show a3) => Symbol -> a3 -> a2 -> H a
tableJoinError name t1 t2 = throwH $ TypeMismatch
  $ "Column " <> toS name <> ": trying to join " <> show t2 <> " onto " <> show t1
{-# INLINABLE tableJoinError #-}


-- | inner join
ij :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
{-# INLINABLE ij #-}
ij = liftToH2 @Table @KeyedTable
  $ \t kt -> bind (V.unzip <$> safeMapAccessH (\hm -> V.imapMaybe (\i -> fmap (IterIndex i,) . (hm HM.!?))) t kt)
  $ \(idxsLeft, idxs) -> mergeTablesPreferSecond (unsafeBackpermute idxsLeft t) $ unsafeBackpermute idxs $ value kt

-- | inner join (cartesian)
ijc :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
{-# INLINABLE ijc #-}
ijc = liftToH2 $ \t (kt :: KeyedTable) -> do
  let groupsToIndices = unsafeGroupingDict kt 
  (idxsGroupLeft, idxsGroupRight) <- V.unzip <$> safeMapAccessH (\hm -> V.imapMaybe (\i -> fmap (IterIndex i,) . (hm HM.!?))) t groupsToIndices
  let (idxsRight, idxsLeft) = groupingToConcatenatedIndicesAndDuplication (value groupsToIndices) idxsGroupRight idxsGroupLeft
  mergeTablesPreferSecond (unsafeBackpermute idxsLeft t) $ unsafeBackpermute idxsRight $ value kt

groupingToConcatenatedIndicesAndDuplication :: Grouping -> Vector IterIndex -> Vector a -> (Vector IterIndex, Vector a)
{-# INLINABLE groupingToConcatenatedIndicesAndDuplication #-}
groupingToConcatenatedIndicesAndDuplication grp grpIdxs vec =
  (V.concat $ toList permutedGroupIdxs
  , V.concat $ V.toList $ V.zipWith (V.replicate . V.length) permutedGroupIdxs vec)
  where permutedGroupIdxs = unsafeBackpermute grpIdxs grp


-- | left join
lj :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
{-# INLINABLE lj #-}
lj = liftToH2 $ \t (kt :: KeyedTable) -> do
  idxs <- safeMapAccessH (\hm -> fmap (hm HM.!?)) t kt
  let fill  = combineCols $ \col1 col2 -> toWrappedDyn $ V.zipWith (\old new -> fromMaybe old new) col1
                                            $ unsafeBackpermuteMaybeV col2 $ coerce idxs
  if all isJust idxs then
    mergeTablesWith untouchedA toMaybe (combineCols $ const toWrappedDyn)
    t $ unsafeBackpermute (Data.Maybe.fromJust <$> idxs) $ value kt
    else mergeTablesWith untouchedA (unsafeBackpermuteMaybeTableCol idxs) fill t $ value kt

-- | left join (cartesian)
ljc :: (ToTable t, ToKeyedTable kt, HasCallStack) => t -> kt -> TableH
{-# INLINABLE ljc #-}
ljc = liftToH2 $ \t (kt :: KeyedTable) -> do
  let groupsToIndices = unsafeGroupingDict kt
  idxsGroupRight <- safeMapAccessH (\hm -> fmap (hm HM.!?)) t groupsToIndices
  let (idxsRight, idxsLeft) = groupingToConcatenatedIndicesAndDuplicationM (value groupsToIndices) idxsGroupRight $
                              V.enumFromN 0 (count t)
      inputPermute _ = pure . mapWrapped (unsafeBackpermute idxsLeft)
      fill  = combineCols $ \col1 col2 -> toWrappedDyn $ unsafeBackpermuteMaybe2 col1 col2 (coerce idxsLeft) $ coerce idxsRight
  if all isJust idxsGroupRight then
    mergeTablesWith inputPermute toMaybe (combineCols $ const toWrappedDyn) t
    $ unsafeBackpermute (Data.Maybe.fromJust <$> idxsRight) $ value kt
    else mergeTablesWith inputPermute (unsafeBackpermuteMaybeTableCol idxsRight) fill t $ value kt


groupingToConcatenatedIndicesAndDuplicationM :: Grouping -> Vector (Maybe IterIndex) -> Vector a -> (Vector (Maybe IterIndex), Vector a)
{-# INLINABLE groupingToConcatenatedIndicesAndDuplicationM #-}
groupingToConcatenatedIndicesAndDuplicationM grp grpIdxs vec =
  (V.concat $ toList $ maybe (V.singleton Nothing) (fmap Just) <$> permutedGroupIdxs
  , V.concat $ V.toList $ V.zipWith (maybe V.singleton (V.replicate . V.length)) permutedGroupIdxs vec)
  where permutedGroupIdxs = unsafeBackpermuteMaybeV grp $ coerce grpIdxs


-- | prepend (first arg) and append (second arg) Nothing's to a column vector (turning it to Maybe as needed)
padColumn :: HasCallStack => Int -> Int -> Symbol -> TableCol -> TableColH
padColumn prependN appendN = modifyMaybeCol $ Just $ TableColModifierWithDefault $ \def -> 
  let  prepend | prependN <= 0 = id
               | True          = (V.replicate prependN def <>)
       append  | appendN <= 0  = id
               | True          = (<> V.replicate appendN def)
  in prepend . append

-- | union join table
ujt :: (ToTable t, ToTable g, HasCallStack) => t -> g -> TableH
{-# INLINABLE ujt #-}
ujt = liftToH2 @Table @Table $ \t1 t2 -> mergeTablesWith (padColumn 0 $ count t2) (padColumn (count t1) 0)
  (combineCols $ \a b -> toWrappedDyn $ a <> b) t1 t2


joinMatchingTables :: (HasCallStack, ToTable a, ToTable b) => a -> b -> TableH
{-# INLINABLE joinMatchingTables #-}
joinMatchingTables = liftToH2 $ orderedColumnIntersection True $ \v1 v2 _ -> toWrappedDyn $ v1 <> v2

fold1TableH :: ToTable t => (TableH -> TableH -> TableH) -> NonEmpty t -> TableH
fold1TableH f = foldl1' f . fmap toH
{-# INLINE fold1TableH #-}


-- | union join keyed table
ujk :: (ToKeyedTable t, ToKeyedTable g, HasCallStack) => t -> g -> KeyedTableH
{-# INLINABLE ujk #-}
ujk = liftToH2 @KeyedTable @KeyedTable $ \kt1 kt2 -> do
 combinedUniqueKeys <- distinct <$> on joinMatchingTables key kt1 kt2
 let (idxs1, idxs2) = fmap (unsafeMap kt1 HM.!?) &&& fmap (unsafeMap kt2 HM.!?) $ toUnsafeKeyVector $ combinedUniqueKeys
     fill = combineCols $ \v1 v2 -> toWrappedDyn $ Hoff.Vector.unsafeBackpermuteMaybe2 v1 v2 (coerce $ Data.Maybe.fromJust <$> idxs1)
                                    $ coerce idxs2
 dictNoLengthCheck combinedUniqueKeys <$> mergeTablesWith
   (unsafeBackpermuteMaybeTableCol idxs1) (unsafeBackpermuteMaybeTableCol idxs2) fill (value kt1) (value kt2)
        

-- -- | union join keyed table (cartesian)
-- ujc :: HasCallStack => KeyedTable -> KeyedTable -> KeyedTable

-- | turns the given columns to I and the other columns to Maybe
-- throwHs TypeMismatch and UnexpectedNulls
fromJusts :: (ToTable t, HasCallStack, ToVector f) => f Symbol -> t -> TableH
{-# INLINABLE fromJusts #-}
fromJusts columns = chainToH g
  where g table | null errs = flip mapMTableWithName table $ \col v ->
                    if V.elem col cvec then fromJustCol (Just table) (Just col) v else toMaybe col v
                | True      = throwH $ KeyNotFound $ ": The following columns do not exist:\n" <> show errs <> "\nAvailable:\n"
                              <> show (cols table)
          where errs = missing cvec $ flipTable table
        cvec = toVector columns

allToMaybe :: (ToTable t, HasCallStack) => t -> TableH
allToMaybe  = chainToH $ mapMTableWithName toMaybe
{-# INLINABLE allToMaybe #-}

allFromJusts :: (ToTable t, HasCallStack) => t -> TableH
allFromJusts = chainToH @Table $ \t -> fromJusts (cols t) t
{-# INLINABLE allFromJusts #-}

-- * convert to statically typed, or assure tables adheres to static schema

keyed :: (HasCallStack, ToTable k, ToTable v) => k -> v -> KeyedTableH
keyed = liftToH2 dict
{-# INLINE keyed #-}

fromKeyed :: (HasCallStack, FromTable k, FromTable v, ToKeyedTable kt) => kt -> (H k, H v)
fromKeyed kt = (fromTable =<< key' kt, fromTable =<< value' kt)
{-# INLINABLE fromKeyed #-}

keyedTypedTable :: forall k v t . (HasCallStack, WrappableTableRow k, WrappableTableRow v, ToKeyedTable t)
  => t -> H (KeyedTypedTable k v)
keyedTypedTable = fmap UnsafeKeyedTypedTable . uncurry keyed . fromKeyed @(TableR k) @(TableR v)
{-# INLINABLE keyedTypedTable #-}

typedTable :: forall r t . (HasCallStack, WrappableTableRow r, ToTable t) => t -> H (TypedTable r)
typedTable = fmap UnsafeTypedTable . toH . chain (fromTable @(TableR r)) . toH
{-# INLINABLE typedTable #-}

-- | no bounds checking
unsafeRowDict :: Table -> Int -> TableRowDict
unsafeRowDict t i = dictNoLengthCheck (cols t) $ withWrapped (toWrappedDyn . I . (flip V.unsafeIndex i)) <$> vals t

rows :: ToH Table t => t -> VectorH TableRowDict
rows = mapToH $ \t -> V.generate (count t) $ unsafeRowDict t
{-# INLINABLE rows #-}

rowsR :: (HasCallStack, TableRow r, ToTable t) => t -> VectorH (Record I r)
rowsR = chainToH @Table $ chain flipRecord . columns
{-# INLINABLE rowsR #-}

instance TableRow r => FromTable (TableR r) where
  fromTable t = R.cmapM (Proxy @Typeable) (\(K name) -> fromWrappedDynF (Just $ toS name) =<< f name) $ R.reifyKnownFields Proxy
    where f = (flipTable t !) . toS
  {-# INLINABLE fromTable #-} 
  
instance (WrappableTableRow r) => ToH Table (TableRH r) where
  toH = chain toH
  {-# INLINE toH #-}

fromRowsI :: forall row r . WrappableTableRowI r => Record ((->) row) r -> Vector row -> TableH
fromRowsI r v = table . fmap (first toS) . R.toList $ R.cmap (Proxy @Wrappable) (K . toWrappedDynI . (<$> v)) r
{-# INLINABLE fromRowsI #-}

instance (WrappableTableRow r) => ToH Table (TableR r) where
  toH = table . fmap (first toS) . R.toList . R.cmap (Proxy @Wrappable3)
            (K . toWrappedDyn)
  {-# INLINABLE toH #-}

-- | special treatment for I
columns :: (ToTable t, HasCallStack, TableRow r) => t -> H (TableR r)
columns = chainToH $ \t -> let f = (flipTable t !) . toS in
  R.cmapM (Proxy @Typeable) (\(K name) -> fromWrappedDyn (Just $ toS name) =<< f name) $ R.reifyKnownFields Proxy
{-# INLINABLE columns #-}

-- | special treatment for I
fromColumnsI :: (WrappableTableRowI r, HasCallStack) => TableR r -> TableH
fromColumnsI = table . fmap (first toS) . R.toList . R.cmap (Proxy @Wrappable)
              (K . toWrappedDynI)
{-# INLINABLE fromColumnsI #-}
  
flipRecord :: (KnownFields r, HasCallStack) => TableR r -> VectorH (Record I r)
flipRecord cs = case equalLength $ uncurry dictNoLengthCheck $ V.unzip
  $ V.fromList $ fmap (first (toS::String -> Symbol)) $ R.toList $ R.map (K . V.length) cs of
  Nothing               -> pure mempty
  Just (n, Nothing)     -> pure $ V.generate n $ \idx -> R.map (\v -> I $ V.unsafeIndex v idx) cs
  Just (_, Just err)    -> throwH $ TableDifferentColumnLenghts err
{-# INLINABLE flipRecord #-}

-- zipRecord :: (a -> b -> x) -> TableR '["a" := a, "b" := b] -> Vector x
-- zipRecord f r = V.zipWith f (R.get #a r) (R.get #b r)

-- zipRecord3 :: (a -> b -> c -> x) -> TableR '["a" := a, "b" := b, "c" := c] -> Vector x
-- zipRecord3 f r = V.zipWith3 f (R.get #a r) (R.get #b r) (R.get #c r)

-- zipRecord4 :: (a -> b -> c -> d -> x) -> TableR '["a" := a, "b" := b, "c" := c, "d" := d] -> Vector x
-- zipRecord4 f r = V.zipWith4 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r)

-- zipRecord5 :: (a -> b -> c -> d -> e -> x) -> TableR '["a" := a, "b" := b, "c" := c, "d" := d, "e" := e] -> Vector x
-- zipRecord5 f r = V.zipWith5 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r) (R.get #e r)

-- zipRecord6 :: (a -> b -> c -> d -> e -> f -> x) -> TableR '["a" := a, "b" := b, "c" := c, "d" := d, "e" := e, "f" := f] -> Vector x
-- zipRecord6 f r = V.zipWith6 f (R.get #a r) (R.get #b r) (R.get #c r) (R.get #d r) (R.get #e r) (R.get #f r)

-- zipTable :: (Typeable a, Typeable b) => (a -> b -> x) -> Table -> Vector x
-- zipTable f = zipRecord f . columns

-- zipTable3 :: (Typeable a, Typeable b, Typeable c) => (a -> b -> c -> x) -> Table -> Vector x
-- zipTable3 f = zipRecord3 f . columns

-- zipTable4 :: (Typeable a, Typeable b, Typeable c, Typeable d) => (a -> b -> c -> d -> x) -> Table -> Vector x
-- zipTable4 f = zipRecord4 f . columns

-- zipTable5 :: (Typeable a, Typeable b, Typeable c, Typeable d, Typeable e) => (a -> b -> c -> d -> e -> x) -> Table -> Vector x
-- zipTable5 f = zipRecord5 f . columns

-- zipTable6 f = zipRecord6 f . columns


summaryKeys :: Vector Text
summaryKeys = toVector ["type"
                       ,"non nulls"
                       ,"nulls"
                       ,"unique"
                       ,"min"
                       ,"max"
                       ,"most frequent"
                       ,"frequency"]

summary :: ToTable t => t -> KeyedTableH
summary = dict (tc' "s" $ summaryKeys) <=< mapMTableWithName summaryCol <=< toH
{-# INLINABLE summary #-}

summaryCol :: Symbol -> TableCol -> TableColH
summaryCol name = \case
  c@(WrappedDyn tr@(App con _) v)
    | Just HRefl <- tr    `eqTypeRep` typeRep @(I None)  -> summaryVec c $ V.empty @()
    | Just HRefl <- con   `eqTypeRep` typeRep @I         -> summaryVec c $ coerceUI v
    | Just HRefl <- con   `eqTypeRep` typeRep @Maybe            -> summaryVec c $ V.catMaybes v
  w             -> errorMaybeOrI (Just name) w

summaryVec :: forall a . Wrappable a => TableCol -> Vector a -> TableColH
{-# INLINABLE summaryVec #-}
summaryVec (WrappedDyn tr fullVec) nonnull = pure $ toWrappedDynI $ toVector
  [shot tr
  ,le nonnull
  ,nulls
  ,le $ distinct nonnull
  ,mm V.minimum
  ,mm V.maximum
  ,fst maxFreq
  ,snd maxFreq]
  where mm f | null nonnull     = "n/a" :: Text
             | True             = toText $ f $ nonnull 
        nulls = toS $ show $ length fullVec - length nonnull
        maxFreq | null nonnull  = ("n/a", "0")
                | True          = toText *** shot $ Data.List.maximumBy (comparing snd) $ HM.toList
                                  $ HM.fromListWith (+) $ toList $ (,1::Int) <$> nonnull
        le = shot . length
        shot = toS . show
        shot :: Show x => x ->  Text
        toText :: a -> Text 
        toText = toS . toLazyText . buildAtomInColumn
