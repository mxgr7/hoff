{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE TemplateHaskell #-}

module Hoff.H where

import           Control.Lens
import qualified Prelude
import           Yahp


-- * Exceptions 

data HoffException where
  TableWithNoColumn          :: HasCallStack => String -> HoffException
  CountMismatch              :: HasCallStack => String -> HoffException
  TypeMismatch               :: HasCallStack => String -> HoffException
  TableDifferentColumnLenghts:: HasCallStack => String -> HoffException
  IncompatibleKeys           :: HasCallStack => String -> HoffException
  IncompatibleTables         :: HasCallStack => String -> HoffException
  KeyNotFound                :: HasCallStack => String -> HoffException
  UnexpectedNulls            :: HasCallStack => String -> HoffException
  -- deriving (Eq, Generic, Typeable)

-- instance Exception HoffException

instance Show HoffException where
  show = \case 
    TableWithNoColumn          t        -> "TableWithNoColumn "         <> t <> "\n" <> prettyCallStack callStack 
    CountMismatch              t        -> "CountMismatch "             <> t <> "\n" <> prettyCallStack callStack
    TypeMismatch               t        -> "TypeMismatch "              <> t <> "\n" <> prettyCallStack callStack
    TableDifferentColumnLenghts t       -> "TableDifferentColumnLenghts\n" <> t <> "\n" <> prettyCallStack callStack
    IncompatibleKeys           t        -> "IncompatibleKeys "          <> t <> "\n" <> prettyCallStack callStack
    IncompatibleTables         t        -> "IncompatibleTables "        <> t <> "\n" <> prettyCallStack callStack
    KeyNotFound                t        -> "KeyNotFound "               <> t <> "\n" <> prettyCallStack callStack
    UnexpectedNulls            t        -> "UnexpectedNulls "           <> t <> "\n" <> prettyCallStack callStack

-- * Hoff Monad

newtype H a = H { runHEither :: Either HoffException a }
  deriving (Functor, Applicative, Monad, Generic)

makeLensesWith (lensField .~ mappingNamer (\f -> [f <> "_"]) $ lensRules) ''H

mapHException :: (HoffException -> HoffException) -> H a -> H a
mapHException = (runHEither_ . _Left %~)

throwH :: HoffException -> H a
throwH = H . Left
{-# INLINABLE throwH #-}

guardH :: HoffException -> Bool -> H ()
guardH e = bool (throwH e) $ pure ()
{-# INLINABLE guardH #-}

runHWith :: (HoffException -> b) -> (a -> b) -> H a -> b
runHWith l r = either l r . runHEither
{-# INLINABLE runHWith #-}

runHMaybe :: H a -> Maybe a
runHMaybe = either (const Nothing) Just . runHEither

noteH :: HoffException -> Maybe a -> H a
noteH e = maybe (throwH e) pure
{-# INLINABLE noteH #-}

unsafeH :: H a -> a
unsafeH = runHWith (throw . Prelude.userError . show) id
{-# INLINABLE unsafeH #-}

instance Show a => Show (H a) where
  show = runHWith show show

instance Eq a => Eq (H a) where
  (==) = on g runHEither
    where g (Right a) (Right b) = a == b
          g _ _ = False

-- class HoffError m where
--   throwHa :: HoffException -> m a

-- type HT = forall m . HoffError m => m (Table)

-- asds :: Table -> HT
-- asds = throwHa $ TableWithNoColumn ""

-- class ToH a b where
--   toH :: b -> H a 

-- instance {-# OVERLAPPABLE #-} ToH a a where
--   toH = pure

-- instance {-# OVERLAPS #-} ToH a (H a) where
--   toH = id
  

-- liftH2 :: (ToH x a, ToH y b) => (x -> y -> H c) -> a -> b -> H c
-- liftH2 f x y = do { a <- toH x; b <- toH y; f a b }

-- mapH :: ToH x a => (x -> c) -> a -> H c
-- mapH f = fmap f . toH

-- liftH :: ToH x a => (x -> H b) -> a -> H b
-- liftH f = chain f . toH

