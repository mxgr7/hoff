{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
module Hoff.HSql where

import           Control.Lens hiding (Empty, (<.))
import           Control.Monad.Reader
import           Data.Record.Anon.Advanced (Record)
import qualified Data.Record.Anon.Advanced as R
import           Data.SOP
import           Data.String
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified GHC.Types
import           Hoff.Dict as H
import           Hoff.H
import           Hoff.Table as H
import           Hoff.Utils
import           Hoff.Vector
import           Type.Reflection as R
import qualified Yahp as Y
import           Yahp hiding (reader, ask, (:.:), group, delete, take, filter, (<|>))


newtype NamedTableReader f a = NamedTableReader { fromNamedTableReader :: (Maybe Symbol, ReaderT (Table, f) H a) }
  deriving (Functor)

makeLensesWith (lensField .~ mappingNamer (\f -> [f <> "_"]) $ lensRules) ''NamedTableReader
makeLensesWith (lensField .~ mappingNamer (\f -> [f <> "_"]) $ lensRules) ''(:.:)

type TRead f = NamedTableReader f :.: Vector
type TReadD f = NamedTableReader f TableCol

type Exp        = TRead ()
type ExpD       = TReadD ()
type Exps       = [ExpD]

data Grouper = Grouper (Vector Int) (forall a . Vector a -> Vector (Vector a))

fromGrouping :: Grouping -> Grouper
fromGrouping i = Grouper (V.length <$> i) $ applyGrouping i

fullGroup :: Iterable t => t -> Grouper
fullGroup t = Grouper (pure $ count t) pure
{-# INLINABLE fullGroup #-}

type Agg        = TRead Grouper
type AggD       = TReadD Grouper
type Aggs       = [AggD]

instance {-# OVERLAPS #-} KnownSymbol m => IsLabel m [Symbol] where
  fromLabel = pure $ fromLabel @m
  {-# INLINABLE fromLabel #-}
  
instance {-# OVERLAPS #-} KnownSymbol m => IsLabel m Symbols where
  fromLabel = pure $ fromLabel @m
  {-# INLINABLE fromLabel #-}
  
instance {-# OVERLAPS #-} KnownSymbol m => IsLabel m Symbol where
  fromLabel = toS $ symbolVal (Proxy @m)
  {-# INLINABLE fromLabel #-}
  
-- * Query types and construction

noName :: ReaderT (Table, f) H (g p) -> (NamedTableReader f :.: g) p
noName = Comp . NamedTableReader . (Nothing,)
{-# INLINABLE noName #-}

withName :: Symbol -> ReaderT (Table, f) H (g p) -> (NamedTableReader f :.: g) p
withName name = Comp . NamedTableReader . (Just name,)
{-# INLINABLE withName #-}

nameLens :: (Maybe Symbol -> Identity (Maybe Symbol)) -> NamedTableReader f a -> Identity (NamedTableReader f a)
nameLens = fromNamedTableReader_ . _1
{-# INLINABLE nameLens #-}

setName :: Symbol -> NamedTableReader f a -> NamedTableReader f a
setName name = nameLens .~ Just name
{-# INLINABLE setName #-}

mapNTR :: (ReaderT (Table, f) H (Vector a1) -> ReaderT (Table, g) H (Vector a2)) -> TRead f a1 -> TRead g a2
mapNTR f = Comp . NamedTableReader . second f . fromNamedTableReader . unComp
{-# INLINABLE mapNTR #-}

runForgetName :: ToTable t => NamedTableReader f p -> (t, f) -> H p
runForgetName (NamedTableReader (_, r)) (t, f) = flip chainToH t $ \tt -> runReaderT r (tt, f)
{-# INLINABLE runForgetName #-}

runForgetNameNameC :: ToTable t => (NamedTableReader f :.: g) p -> (t, f) -> H (g p)
runForgetNameNameC = runForgetName . unComp
{-# INLINABLE runForgetNameNameC #-}

instance Applicative (NamedTableReader f) where
  pure = NamedTableReader . (Nothing,) . pure
  (NamedTableReader (nf,f)) <*> (NamedTableReader (nx,x)) = NamedTableReader (nx `mplus` nf, f <*> x)
  {-# INLINABLE pure #-}
  
-- ** Exp 

-- *** automatically named
ai :: (Wrappable a) => TRead f a -> TReadD f
ai = fmap toWrappedDynI . unComp
{-# INLINE ai #-}

am :: (Wrappable a) => TRead f (Maybe a) -> TReadD f
am = af
{-# INLINE am #-}

af :: forall g a f . (Wrappable a, Wrappable1 g) => TRead f (g a) -> TReadD f
af = fmap toWrappedDyn . unComp
{-# INLINE af #-}

-- -- *** explicitly named

ei :: (Wrappable a) => Symbol -> TRead f a -> TReadD f
ei name = setName name . ai
{-# INLINE ei #-}

(</) :: (HasCallStack, Wrappable a) => Symbol -> TRead f a -> TReadD f
(</) = ei
{-# INLINE (</) #-}

ef :: forall g a f . (Wrappable1 g, Wrappable a) => Symbol -> TRead f (g a) -> TReadD f
ef name = setName name . af
{-# INLINE ef #-}

em :: (Wrappable a) => Symbol -> TRead f (Maybe a) -> TReadD f
em = ef
{-# INLINE em #-}
                
sn :: Symbol -> TReadD f -> TReadD f
sn = setName
{-# INLINE sn #-}

-- ** Exp sources

-- *** Constants

co :: a -> TRead f a
co x = noName $ reader $ \(t,_) -> V.replicate (count t) x
{-# INLINE co #-}

ca :: Agg a -> Exp a
ca = mapNTR $ \agr -> ReaderT $ \(t,_) -> V.replicate (count t) . V.head <$> runReaderT agr (t, fullGroup t)
{-# INLINE ca #-}

fby :: Agg a -> Exps -> Exp a
fby agg bys = flip mapNTR agg $ \agg' -> ReaderT $ \(t, ()) -> do
  idxs <- snd . getGroupsAndGrouping <$> select bys t
  broadcastGroupValue idxs <$> runReaderT agg' (t, fromGrouping idxs)
{-# INLINABLE fby #-}

-- *** external vectors
vec :: Vector a -> TRead f a
vec v = noName $ reader $ const v

vi :: Wrappable a => Symbol -> Vector a -> TReadD f
vi n = ei n . vec

vf :: forall g a f . (Wrappable1 g, Wrappable a) => Symbol -> Vector (g a) -> TReadD f
vf n = ef n . vec

-- *** Table columns

ac :: (HasCallStack, Typeable a) => Symbol -> Exp a
ac = tableCol
{-# INLINABLE ac #-}

aa :: (HasCallStack, Typeable a) => Symbol -> Agg (Vector a)
aa = tableColAgg
{-# INLINABLE aa #-}

ad :: HasCallStack => Symbol -> ExpD
ad = tableColD

ed :: HasCallStack => Symbol -> Symbol -> ExpD
ed n = setName n . ad

-- | unrwap I, leave other constructors untouched
tableCol :: (HasCallStack, Typeable a) => Symbol -> Exp a
tableCol n = liftC' (fromWrappedDyn $ Just n) $ tableColD n
{-# INLINABLE tableCol #-}

tableColD :: HasCallStack => Symbol -> ExpD
tableColD name = NamedTableReader (Just name, ReaderT $ \t -> flipTable (fst t) ! name)
{-# INLINE tableColD #-}

-- test :: HasCallStack => Exp Int
-- test = noName $ ReaderT $ \t -> fromWrappedDyn =<< flipTable (fst t) ! "asd"

-- -- | this cannot be done currectly in the current design because (I
-- -- None) columns need to remain castable to Maybe a for any a.
-- -- To achieve this, we would need to add Vector to the allowed constructors inside a Wrappable
-- tableColAggD :: (HasCallStack) => Symbol -> AggD
-- tableColAggD name = NamedTableReader (Just name, ReaderT $ \(t, Grouper _ grouper) ->
--                                          asd grouper <$> flipTable t ! name)
-- {-# INLINABLE tableColAggD #-}

-- asd :: (forall a . Vector a -> Vector (Vector a)) -> TableCol -> TableCol
-- asd grouper = \case
--   c@(WrappedDyn tr@(App con _) v)
--     | Just HRefl <- tr    `eqTypeRep` typeRep @(I None) -> pure mempty
--     | Just HRefl <- con   `eqTypeRep` typeRep @I        -> fromWrappedDyn c
--     | Just HRefl <- con   `eqTypeRep` typeRep @Maybe    -> fromWrappedDyn $ toWrappedDynI $ V.catMaybes v
--   w             -> errorMaybeOrI "n/a" w


tableColAgg :: (HasCallStack, Typeable a) => Symbol -> Agg (Vector a)
tableColAgg name = withName name $ ReaderT $ \(t, Grouper _ grouper) -> fmap grouper
  $ fromWrappedDyn (Just name) =<< flipTable t ! name
{-# INLINABLE tableColAgg #-}


instance IsString ExpD where
  fromString :: HasCallStack => String -> ExpD
  fromString = tableColD . toS
  
instance {-# OVERLAPS #-} (KnownSymbol m) => IsLabel m Exps where
  fromLabel :: HasCallStack => Exps
  fromLabel = [fromLabel @m]
  {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (KnownSymbol m) => IsLabel m (ExpD) where
  fromLabel :: HasCallStack => ExpD
  fromLabel = tableColD $ fromLabel @m
  {-# INLINABLE fromLabel #-}

instance Typeable a => IsString (Exp a) where
  fromString :: HasCallStack => String -> Exp a
  fromString = tableCol . toS
  {-# INLINABLE fromString #-}
  
instance {-# OVERLAPS #-} (Typeable a, KnownSymbol m) => IsLabel m (Exp a) where
  fromLabel :: HasCallStack => Exp a
  fromLabel = tableCol $ fromLabel @m
  {-# INLINABLE fromLabel #-}
  
-- instance (KnownSymbol m) => IsLabel m AggD where
--   fromLabel :: HasCallStack => AggD
--   fromLabel = tableColAggD $ fromLabel @m
--   {-# INLINABLE fromLabel #-}

instance {-# OVERLAPS #-} (Typeable a, KnownSymbol m) => IsLabel m (Agg (Vector a)) where
  fromLabel :: HasCallStack => Agg (Vector a)
  fromLabel = tableColAgg $ fromLabel @m
  {-# INLINABLE fromLabel #-}

instance Typeable a => IsString (Agg (Vector a)) where
  fromString = tableColAgg . toS
  {-# INLINABLE fromString #-}
  

-- * Query execution


type Where = Exp Bool

data WhereResult = Everything | Empty | ProperSubset (Vector Bool) (Vector IterIndex)
  deriving (Show, Eq, Ord)

invert :: WhereResult -> WhereResult
invert = \case
  Everything            -> Empty
  Empty                 -> Everything
  ProperSubset m _      -> properSubset $ not <$> m

properSubset :: Vector Bool -> WhereResult
properSubset m = ProperSubset m $ V.map IterIndex $ V.findIndices id m

fromMask :: Vector Bool -> WhereResult
fromMask mask   | V.and mask            = Everything
                | not (V.or mask)       = Empty
                | True                  = properSubset mask

runWhere :: ToTable t => Where -> t -> H WhereResult
runWhere wh = chainToH @Table $ \t -> fromMask <$> runForgetNameNameC wh (t, ())
{-# INLINABLE runWhere #-}

applyWhereResultUnsafe :: ToTable t => t -> WhereResult -> TableH
applyWhereResultUnsafe = flip $ \wh -> mapToH $ \t -> case wh of
  Everything            -> t
  Empty                 -> H.take 0 t
  ProperSubset _ idcs   -> unsafeBackpermute idcs  t 
{-# INLINABLE applyWhereResultUnsafe #-}
  
filter :: (HasCallStack, ToTableAndBack t) => Where -> t -> H' t
filter wh = useToTableAndBack $ \t -> applyWhereResultUnsafe t =<< runWhere wh t
{-# INLINABLE filter #-}

toFiltered :: (HasCallStack, ToTable t) => (a -> Table -> H c) -> a -> Where -> t -> H c
toFiltered x a wh = chainToH @Table $ x a <=< filter wh 
{-# INLINE toFiltered #-}

toFiltered2 :: (HasCallStack, ToTable t) => (t1 -> t2 -> Table -> H c) -> t1 -> t2 -> Where -> t -> H c
toFiltered2 x a b wh = chainToH @Table $ x a b <=< filter wh 
{-# INLINE toFiltered2 #-}

toFiltered3 :: (HasCallStack, ToTable t) => (t1 -> t2 -> t3 -> Table -> H c) -> t1 -> t2 -> t3 -> Where -> t -> H c
toFiltered3 x a b c wh = chainToH @Table $ x a b c <=< filter wh 
{-# INLINE toFiltered3 #-}



-- ** delete

deleteW :: (HasCallStack, ToTableAndBack t) => Where -> t -> H' t
deleteW = useToTableAndBack . selectW [] . fmap not
{-# INLINABLE deleteW #-}

delete :: (HasCallStack, ToTableAndBack t) => ToVector f => f Symbol -> t -> H' t
delete s = useToTableAndBack $ chainToH $ tableNoLengthCheck . H.deleteKeys (toVector s) . flipTable
{-# INLINABLE delete #-}

-- ** select
--
-- todo:
-- select[n]
-- select[m n]
-- select[order]
-- select[n;order]
-- select distinct

select :: (ToTableAndBack t, HasCallStack) => Exps -> t -> H' t
{-# INLINABLE select #-}
select e = useToTableAndBack $ chainToH $ \t -> case e of
  [] -> toH t
  e  -> tableAnonymous =<< mapM (mapM (flip runReaderT (t, ())) . fromNamedTableReader) e

selectW :: (ToTableAndBack t, HasCallStack) => Exps -> Where -> t -> H' t
selectW e = useToTableAndBack . toFiltered select e
{-# INLINE selectW #-}

selectBy :: (ToTable t, HasCallStack) => Aggs -> Exps -> t -> KeyedTableH
selectBy aggs bys t = uncurry dictNoLengthCheck . snd <$> selectByRaw' aggs bys (toH t)
{-# INLINABLE selectBy #-}

selectByRaw' :: HasCallStack => Aggs -> Exps -> TableH -> H (Grouping, (Table, Table))
selectByRaw' aggs bys t = select bys t >>= \bys' -> traverse2 tableAnonymous =<< selectByRaw aggs bys' t
{-# INLINABLE selectByRaw' #-}

selectByRaw :: (ToTable tt, HasCallStack) => (HashableDictComponent t, Traversable f)
  => f AggD -> t -> tt -> H (Grouping, (t, (f (Maybe Symbol, TableCol))))
selectByRaw aggs by = chainToH $ \t -> (idxs,) <$> (keys,) <$> forM aggs (mapM (flip runReaderT (t, fromGrouping idxs)) . fromNamedTableReader)
  where (keys, idxs) = getGroupsAndGrouping by
{-# INLINABLE selectByRaw #-}


selectByW :: (ToTable t, HasCallStack) => Aggs -> Exps -> Where -> t -> KeyedTableH
selectByW = toFiltered2 selectBy
{-# INLINE selectByW #-}

selectAgg :: (ToTable t, HasCallStack) => Aggs -> t -> TableH
selectAgg aggs = chainToH $ \t -> tableAnonymous =<< forM aggs (mapM (flip runReaderT (t, fullGroup t)) . fromNamedTableReader)
{-# INLINABLE selectAgg #-}

selectAggW :: (ToTable t, HasCallStack) => Aggs -> Where -> t -> TableH
selectAggW = toFiltered selectAgg
{-# INLINE selectAggW #-}


-- ** update

update :: (ToTableAndBack t, HasCallStack) => Exps -> t -> H' t
update e = useToTableAndBack $ \t -> mergeTablesPreferSecond t =<< select e t
{-# INLINABLE update #-}

-- | new columns are converted to Maybe (if they are not already) to be able to represent missing value
-- that could appear based on data
updateW :: (ToTable t, HasCallStack) => Exps -> Where -> t -> TableH
{-# INLINABLE updateW #-}
updateW e wh = chainToH $ \t -> bind (runWhere wh t) $ \case
  Everything            -> update e t
  Empty                 -> pure t
  ProperSubset mask _   ->
    let pick name (WrappedDyn t new) = fmap2 (WrappedDyn t . conditional mask new) $ fromWrappedDyn $ Just name
        sparseNew _ (WrappedDyn (App con _) col)
          | Just HRefl <- con `eqTypeRep` R.typeRep @Maybe      = pure $ toWrappedDyn $ V.zipWith
            (\m n -> if m then n else Nothing) mask col
          | Just HRefl <- con `eqTypeRep` R.typeRep @I          = pure $ toWrappedDyn $ V.zipWith
            (\m n -> if m then Just n else Nothing) mask $ coerceUI col
        sparseNew name          w                             = errorMaybeOrI (Just name) w
    in flip (mergeTablesWith sparseNew untouchedA pick) t =<< select e t


updateBy :: (ToTableAndBack t, HasCallStack) => Aggs -> Exps -> t -> H' t
updateBy aggs bys = useToTableAndBack $ \t -> selectByRaw' aggs bys t >>= \(idxs, (_, aggTable)) ->
  mergeTablesPreferSecond t (broadcastGroupValue idxs aggTable)
{-# INLINABLE updateBy #-}

-- ** exec

execD :: (ToTable t, HasCallStack) => ExpD -> t -> TableColH
execD es = runForgetName es . (,())
{-# INLINABLE execD #-}

execDW :: (ToTable t, HasCallStack) => ExpD -> Where -> t -> TableColH
execDW = toFiltered execD
{-# INLINE execDW #-}

exec :: forall a t . (ToTable t, HasCallStack) => Exp a -> t -> VectorH a
exec es = runForgetNameNameC es . (,())
{-# INLINABLE exec #-}

execW :: forall a t . (ToTable t, HasCallStack) => Exp a -> Where -> t -> VectorH a
execW = toFiltered exec
{-# INLINE execW #-}


execC :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Exp r -> t -> TableRH r
execC es t = R.mapM (\e -> runForgetNameNameC e (t, ())) es
{-# INLINABLE execC #-}

execCW :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Exp r -> Where -> t -> TableRH r
execCW = toFiltered execC
{-# INLINE execCW #-}

-- | map to row
execRBy :: forall g r t . (ToTable t, HasCallStack, TableRow r, Wrappable g)
        => Record Agg r -> Exp g -> t -> VectorDictH g (Record I r)
execRBy aggs by t = (getGroupsAndGrouping <$> exec by t) >>= \(keys, idxs) ->
  fmap (dictNoLengthCheck keys) $ flipRecord =<< R.mapM (flip runForgetNameNameC (t, fromGrouping idxs)) aggs
{-# INLINABLE execRBy #-}

execRByW :: forall g r t . (ToTable t, HasCallStack, TableRow r, Wrappable g)
        => Record Agg r -> Exp g -> Where -> t -> VectorDictH g (Record I r)
execRByW = toFiltered2 execRBy
{-# INLINE execRByW #-}

execRAgg :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Agg r -> t -> H (Record I r)
execRAgg aggs = chainToH @Table $ \t -> R.mapM (fmap (I . V.head) . flip runForgetNameNameC (t, fullGroup t)) aggs
{-# INLINABLE execRAgg #-}

execRAggW :: forall r t . (ToTable t, HasCallStack, TableRow r) => Record Agg r -> Where -> t -> H (Record I r)
execRAggW = toFiltered execRAgg
{-# INLINABLE execRAggW #-}

-- | map
execMap :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Exp v -> Exp g -> t -> VectorDictH g v
execMap va ke t = dictNoLengthCheck <$> exec ke t <*> exec va t
{-# INLINABLE execMap #-}

execKey :: forall v g t . (ToKeyedTable t, HasCallStack, Wrappable g, Wrappable v) => Exp v -> Exp g -> t -> VectorDictH g v
execKey va ke = chainToH @KeyedTable $ \kt -> dictNoLengthCheck <$> exec ke (key kt) <*> exec va (value kt)
{-# INLINABLE execKey #-}

execBy :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Agg v -> Exp g -> t -> VectorDictH g v
execBy agg by t = do
  bys <- exec by t
  (_, (key, val)) <- selectByRaw (I $ ai agg) bys t
  dictNoLengthCheck key <$> fromWrappedDyn Nothing (snd $ unI val)
{-# INLINABLE execBy #-}

execByW :: forall v g t . (ToTable t, HasCallStack, Wrappable g, Wrappable v) => Agg v -> Exp g -> Where -> t -> VectorDictH g v
execByW = toFiltered2 execBy
{-# INLINE execByW #-}

execAgg :: forall a t . (ToTable t, HasCallStack) => Agg a -> t -> H a
execAgg ag = chainToH @Table $ \t -> V.head <$> runForgetNameNameC ag (t, fullGroup t)
{-# INLINABLE execAgg #-}

execAggW :: forall a t . (ToTable t, HasCallStack) => Agg a -> Where -> t -> H a
execAggW = toFiltered execAgg
{-# INLINABLE execAggW #-}


-- -- fmapWithErrorContext :: (a -> b) -> Exp a -> Exp b
-- -- fmapWithErrorContext f = V.imap (\i a -> f a

-- -- fromJust :: Exp (Maybe a) -> Exp a
-- -- fromJust me = ReaderT $ \t -> f t $ runReaderT me t
-- --   where f table mE = let res = V.catMaybes mE
-- --           in if length res == length mE then res
-- --           else do throwH $ UnexpectedNulls $ "\n" <> show (unsafeBackpermute (IterIndex <$> V.findIndices isNothing mE) table)
  

-- * Sorting

sortT :: forall e t . (HasCallStack, ToTable t) => Exp e -> Comparison e -> t -> TableH
sortT w by = chainToH $ sortByWithM (runForgetNameNameC w . (,())) by
{-# INLINABLE sortT #-}

ascT :: forall e t . (HasCallStack, ToTable t) => (Eq e, Ord e) => Exp e -> t -> TableH
ascT w = sortT w compare
{-# INLINE ascT #-}

descT :: forall e t . (HasCallStack, ToTable t) => (Eq e, Ord e) => Exp e -> t -> TableH
descT w = sortT w $ flip compare
{-# INLINE descT #-}

sortTD :: (HasCallStack, ToTable t) => ExpD -> (forall f a . (Ord1 f, Ord a) => f a -> f a -> Ordering) -> t -> TableH
sortTD w by = chainToH $ \t -> withWrapped (\v -> sortByWith (const v) by t) <$> runForgetName w (t,())
{-# INLINABLE sortTD #-}

ascTD :: (HasCallStack, ToTable t) => ExpD -> t -> TableH
ascTD w = sortTD w compare1
{-# INLINABLE ascTD #-}

descTD :: (HasCallStack, ToTable t) => ExpD -> t -> TableH
descTD w = sortTD w $ flip compare1
{-# INLINABLE descTD #-}

ascC :: TableCol -> TableCol
ascC = mapWrapped (sortByWith id compare1)

descC :: TableCol -> TableCol
descC = mapWrapped (sortByWith id $ flip compare1)




-- * operator constructors

liftC :: (TableCol -> H a) -> TReadD f -> NamedTableReader f a
liftC f = fromNamedTableReader_ . _2 %~ mapReaderT (chain f)
{-# INLINE liftC #-}

liftCWithName :: (Maybe Symbol -> TableCol -> H a) -> TReadD f -> NamedTableReader f a
liftCWithName f (NamedTableReader (n, r)) = NamedTableReader . (n,) $ mapReaderT (chain $ f n) r
{-# INLINE liftCWithName #-}

liftV2 :: (a -> b -> c) -> TRead t a -> TRead t b -> TRead t c
liftV2 = liftE2 . V.zipWith
{-# INLINE liftV2 #-}

liftV3 :: (a -> b -> c -> x) -> TRead t a -> TRead t b -> TRead t c -> TRead t x
liftV3 = liftE3 . V.zipWith3
{-# INLINE liftV3 #-}


liftE3 :: (Vector a -> Vector b -> Vector c -> Vector x) -> TRead t a -> TRead t b -> TRead t c -> TRead t x
liftE3 f (Comp a) (Comp b) (Comp c) = Comp $ liftA3 f a b c
{-# INLINE liftE3 #-}

liftE2 :: (Vector a -> Vector b -> Vector c) -> TRead t a -> TRead t b -> TRead t c
liftE2 f (Comp a) (Comp b) = Comp $ liftA2 f a b
{-# INLINE liftE2 #-}

liftEH :: (Vector a -> VectorH b) -> TRead t a -> TRead t b
liftEH f = Comp . (fromNamedTableReader_ . _2 %~ mapReaderT (chain f)) . unComp
{-# INLINE liftEH #-}

liftE :: (Vector a -> Vector b) -> TRead t a -> TRead t b
liftE f = Comp . fmap f . unComp
{-# INLINE liftE #-}

liftE1flip :: (a2 -> a1 -> c) -> a1 -> TRead t a2 -> TRead t c
liftE1flip = liftE1 . flip
{-# INLINE liftE1flip #-}

liftE1 :: (a1 -> a2 -> c) -> a1 -> TRead t a2 -> TRead t c
liftE1 f = mapE . f
{-# INLINE liftE1 #-}

liftC' :: (TableCol -> VectorH p) -> TReadD f -> TRead f p
liftC' f = Comp . liftC f
{-# INLINABLE liftC' #-}

liftCWithName' :: (Maybe Symbol -> TableCol -> VectorH p) -> TReadD f -> TRead f p
liftCWithName' f = Comp . liftCWithName f
{-# INLINABLE liftCWithName' #-}

mapED :: (forall a g. (Wrappable a, Wrappable1 g) => (g a) -> c) -> ExpD -> Exp c
mapED f = Comp . (fromNamedTableReader_ . _2 . mapped %~ withWrapped (fmap f))
{-# INLINABLE mapED #-}

mapE :: (a -> b) -> TRead t a -> TRead t b
mapE = fmap
{-# INLINE mapE #-}

mapA1flip :: (Vector a -> x -> b) -> x -> Exp a -> Agg b
mapA1flip f = mapA . flip f
{-# INLINE mapA1flip #-}

mapA1 :: (x -> Vector a -> b) -> x -> Exp a -> Agg b
mapA1 f = mapA . f
{-# INLINE mapA1 #-}

mapA :: (Vector a -> b) -> Exp a -> Agg b
mapA f = Comp . commonAgg (\grps -> fmap2 f grps) . unComp
{-# INLINE mapA #-}

mapD0 :: (forall a g. (Wrappable a, Wrappable1 g) => Vector (g a) -> b) -> ExpD -> Agg b
mapD0 f = Comp . commonAgg (\grps -> withWrapped (fmap f . grps))
{-# INLINE mapD0 #-}

mapD1 :: (forall a g. (Wrappable a, Wrappable1 g) => Vector (g a) -> g a) -> ExpD -> AggD
mapD1 f = commonAgg $ \grps -> withWrapped' (\tr -> WrappedDyn tr . fmap f . grps)
{-# INLINE mapD1 #-}

commonAgg :: ((forall a . (Vector a -> Vector (Vector a))) -> a1 -> a2) -> NamedTableReader () a1 -> NamedTableReader Grouper a2
commonAgg f = fromNamedTableReader_ . _2 %~ ReaderT . g
  where g expr (t, Grouper _ grouper) = f grouper <$> runReaderT expr (t,())
{-# INLINABLE commonAgg #-}
