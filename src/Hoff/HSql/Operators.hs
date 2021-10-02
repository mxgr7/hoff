{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

module Hoff.HSql.Operators where

import qualified Data.HashSet as HS
import qualified Data.Maybe as P
import qualified Data.Text as T
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Hoff.Dict
import           Hoff.H as H
import           Hoff.HSql as H
import           Hoff.HSql.TH as H
import           Hoff.Iterable as H
import           Hoff.Table
import           Hoff.Vector
import qualified Prelude as Unsafe
import           Yahp hiding (liftA, delete)

betweenII :: Ord a => TRead t a -> TRead t a -> TRead t a -> TRead t Bool
betweenII = liftV3 $ \a b c -> b <= a && a <= c
{-# INLINABLE betweenII #-}

betweenIE :: Ord a => TRead t a -> TRead t a -> TRead t a -> TRead t Bool
betweenIE = liftV3 $ \a b c -> b <= a && a < c
{-# INLINE betweenIE #-}

betweenEI :: Ord a => TRead t a -> TRead t a -> TRead t a -> TRead t Bool
betweenEI = liftV3 $ \a b c -> b < a && a <= c
{-# INLINABLE betweenEI #-}

betweenEE :: Ord a => TRead t a -> TRead t a -> TRead t a -> TRead t Bool
betweenEE = liftV3 $ \a b c -> b < a && a < c
{-# INLINABLE betweenEE #-}

bool_ :: TRead t Bool -> TRead t x -> TRead t x -> TRead t x
bool_ = liftE3 conditional

rowDict :: Exp TableRowDict
rowDict = noName $ ReaderT $ rows . fst

-- * Maybe 
fromJust_ :: Exp (P.Maybe c) -> Exp c
fromJust_ = fmap P.fromJust

fromJustD :: TReadD t -> TReadD t
fromJustD = liftCWithName $ fromJustCol Nothing

fromMaybe_ :: c -> Exp (P.Maybe c) -> Exp c
fromMaybe_ = liftE1 P.fromMaybe

-- * extract important groups (from Maybe and Either cols)

groupByCol :: forall t a . (ToTableAndBack t, Eq a, Hashable a) => Exp a -> t -> VectorDictH a (InH t)
groupByCol by t1 = do
  t <- t2
  unsafeValue_ (traverse $ back . pure) =<< (flip groupBy_ t <$> exec by t)
  where (t2, back) = toTableAndBack t1
{-# INLINABLE groupByCol #-}

-- | (Lefts, Rights)
-- Requires explicit left type to prove constraint
partitionEithers_ :: (ToTable t, Wrappable left) => Proxy left -> Symbol -> t -> H (Table, Table)
partitionEithers_ p col = chainToH $ \t -> do
  gs <- groupByCol (isRight_ colE) t
  let up f x = update [liftC f colE] $ fromMaybe (H.take 0 t) $ gs !? x
  (,) <$> up (fromLeftColUnsafe p) False <*> up fromRightColUnsafe True
  where colE = tableColD col
{-# INLINE partitionEithers_ #-}

lefts_ :: (ToTable t, Wrappable left) => Proxy left -> Symbol -> t -> TableH
lefts_ p c = fmap fst . partitionEithers_ p c
{-# INLINABLE lefts_ #-}

rights_ :: ToTable t => Symbol -> t -> TableH
rights_ c = fmap snd . partitionEithers_ (Proxy @()) c
{-# INLINABLE rights_ #-}

isRight_ :: ExpD -> Exp Bool
isRight_ = liftC' isRightCol

-- | (Nothings, Justs)

-- | (Nothings, Justs)
-- todo: speed up by using where instead of group to profit from no-op 
partitionMaybes :: ToTable t => Symbol -> t -> (H Table, H Table)
partitionMaybes col t = (delete [col]           =<< applyWhereResultUnsafe t . invert =<< whereJust
                        ,update [fromJustD $ ad col] =<< applyWhereResultUnsafe t =<< whereJust)
  where whereJust = runWhere (isJust_ $ ad col) t
{-# INLINE partitionMaybes #-}
        

isJust_ :: ExpD -> Exp Bool
isJust_ = liftCWithName' isJustCol

isNothing_ :: ExpD -> Exp Bool
isNothing_ = fmap not . isJust_

-- | return rows whete column is Just x and cast to x
justs :: ToTable t => Symbol -> t -> TableH
justs c         = snd . partitionMaybes c
{-# INLINABLE justs #-}

-- | return rows whete column is Nothing and delete the column
nothings :: ToTable t => Symbol -> t -> TableH
nothings c      = fst . partitionMaybes c
{-# INLINABLE nothings #-}
-- * Table Expressions

-- | row index
idx :: Exp Int
idx  = withName "i" $ reader $ V.enumFromN 0 . count . fst

-- * Aggregators

countA :: Agg Int
countA = withName "count" $ reader $ \(_,Grouper l _) -> l

countDistinct :: ExpD -> Agg Int
countDistinct = mapD0 f
  where f :: forall g a . (Wrappable a, Wrappable1 g) => Vector (g a) -> Int
        f = length . HS.fromList . coerceWrapInstances . toList

-- * lookups

(!.) :: (Eq a, Typeable a, Hashable a, Show a) => VectorDict a b -> TRead t a -> TRead t b
(!.) d1 = liftEH (d1 !!) 

(?.) :: (Eq a, Hashable a) => VectorDict a v -> TRead t a -> TRead t (Maybe v)
(?.) d1 = liftE (d1 !!?) 

-- * instances

instance Fractional a => Fractional (TRead f a) where
  (/)           = liftV2 (/)
  recip         = fmap recip
  fromRational  = co . fromRational
  
instance Num a => Num (TRead f a) where
  (+)           = liftV2 (+)
  (*)           = liftV2 (*)
  (-)           = liftV2 (-)
  negate        = fmap negate
  abs           = fmap abs
  signum        = fmap signum
  fromInteger   = co . fromInteger
  
instance {-# OVERLAPS #-} Eq (TRead f a) where
  (==) = notImpl "(==)"

instance {-# OVERLAPS #-} Ord (TRead f a) where
  compare = notImpl "compare"

instance Num a => Real (TRead f a) where
  toRational = notImpl "toRational"

instance Floating a => Floating (TRead f a) where
  pi = co pi
  (**)          = liftV2 (**)
  logBase       = liftV2 logBase
  cos           = fmap cos
  acosh         = fmap acosh
  asin          = fmap asin
  asinh         = fmap asinh
  atan          = fmap atan
  atanh         = fmap atanh
  acos          = fmap acos
  cosh          = fmap cosh
  exp           = fmap exp
  log           = fmap log
  sin           = fmap sin
  sinh          = fmap sinh
  sqrt          = fmap sqrt
  tan           = fmap tan
  tanh          = fmap tanh

instance ConvertText a b => ConvertText (TRead f a) (TRead f b) where
  toS = fmap toS

instance Enum a => Enum (TRead f a) where
  succ          = fmap succ
  pred          = fmap pred
  toEnum        = co . toEnum
  fromEnum      = notImpl "fromEnum"

instance Integral a => Integral (TRead f a) where
  quot          = liftV2 quot   
  rem           = liftV2 rem    
  div           = liftV2 div    
  mod           = liftV2 mod    
  quotRem       = notImpl "quotRem"
  divMod        = notImpl "divMod"
  toInteger     = notImpl "toInteger"

properFraction_ :: (RealFrac a, Integral b) => TRead t a -> TRead t (b, a)
properFraction_ = mapE Yahp.properFraction

truncate_ :: (RealFrac a, Integral b) => TRead t a -> TRead t b
truncate_       = mapE Yahp.truncate

round_ :: (RealFrac a, Integral b) => TRead t a -> TRead t b
round_          = mapE Yahp.round

ceiling_ :: (RealFrac a, Integral b) => TRead t a -> TRead t b
ceiling_        = mapE Yahp.ceiling

floor_ :: (RealFrac a, Integral b) => TRead t a -> TRead t b
floor_          = mapE Yahp.floor

length_ :: forall v f a t . (v ~ f a, Foldable f) => TRead t v -> TRead t Int
length_ = mapE length

instance {-# OVERLAPS #-} Semigroup a => Semigroup (TRead f a) where
  (<>) = liftV2 (<>)

instance {-# OVERLAPS #-} Monoid a => Monoid (TRead f a) where
  mempty = co mempty

instance Bounded a => Bounded (TRead f a) where
  minBound = co minBound
  maxBound = co maxBound

notImpl :: HasCallStack => String -> a
notImpl n = Unsafe.error $ "'TRead f' has no " <> n <> " implementation."

-- * lifted

$(concatMapM (mapA1flip_ "_" . fmap Just)
   [('(V.!)             , "at_")
   ,('(V.!?)            , "lookup_")
   ])

-- $(concatMapM (mapA1flip_ "_" . (,Nothing))
-- [ 
--    ])

$(concatMapM (mapA1_ "_")
-- Vector
   ['V.all
   ,'V.any
   ,'V.maximumBy
   ,'V.minimumBy
   ])

$(concatMapM (mapA_ "A")
  ['V.maximum
  ,'V.minimum
  ,'V.last
  ,'V.and
  ,'V.or
  ,'V.sum
  ,'V.product
  ,'V.head])
  
$(concatMapM (appFs 'mapD1 "D")
  ['V.last
  ,'V.head])

$(concatMapM (liftV2_ "." . (,Nothing))
  ['(&&)
  ,'(||)
  ,'(==)
  ,'(<)
  ,'(<=)
  ,'(>)
  ,'(>=)
  ,'(+)
  ,'(-)
  ,'(*)
  ])

$(concatMapM (liftV2_ "_" . (,Nothing))
-- Prelude
  ['max
  ,'min
  ,'quotRem
  ,'divMod
  ,'compare
  -- Text
  ,'T.cons
  ,'T.snoc
  ,'T.append
  ])

$(concatMapM (liftV2_ "" . fmap Just)
  [
  ])


$(concatMapM (appFs 'mapE "T")
-- Text
  ['T.pack
  ,'T.unpack
  ,'T.singleton
  ,'T.uncons
  ,'T.unsnoc
  ,'T.head
  ,'T.last
  ,'T.tail
  ,'T.init
  ,'T.null
  ,'T.length
  ,'T.transpose
  ,'T.reverse
  ,'T.toCaseFold
  ,'T.toLower
  ,'T.toUpper
  ,'T.toTitle
  ,'T.strip
  ,'T.stripStart
  ,'T.stripEnd
  ,'T.lines
  ,'T.words
  ,'T.unlines
  ,'T.unwords
  ])

replaceT :: Text -> TRead t Text -> TRead t Text -> TRead t Text
replaceT x = liftV2 $ T.replace x

zipWithT :: (Char -> Char -> Char) -> TRead t Text -> TRead t Text -> TRead t Text
zipWithT x = liftV2 $ T.zipWith x

$(concatMapM (appFs 'liftE1 "T")
-- Text
  ['T.intercalate
  ,'T.compareLength
  ,'T.intersperse
  ,'T.take
  ,'T.takeEnd
  ,'T.drop
  ,'T.dropEnd
  ,'T.takeWhile
  ,'T.takeWhileEnd
  ,'T.dropWhile
  ,'T.dropWhileEnd
  ,'T.dropAround
  ,'T.splitAt
  ,'T.breakOn
  ,'T.breakOnEnd
  ,'T.break
  ,'T.span
  ,'T.split
  ,'T.chunksOf
  ,'T.isPrefixOf
  ,'T.isSuffixOf
  ,'T.isInfixOf
  ,'T.stripPrefix
  ,'T.stripSuffix
  ,'T.commonPrefixes
  ,'T.filter
  ,'T.find
  ,'T.elem
  ,'T.partition
  ,'T.index
  ,'T.findIndex
  ,'T.count
  ])

$(concatMapM (appFs 'liftV2 "T")
  ['T.zip
  ])

$(concatMapM (appFs 'co "T")
  ['T.empty
  ])
