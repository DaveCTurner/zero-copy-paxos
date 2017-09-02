--
-- Copyright 2017 David Turner
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--    http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--


module Main where

import           Development.Shake
import           Development.Shake.Command
import           Development.Shake.FilePath
import           Development.Shake.Util

optFlag :: String -> String
optFlag "release" = "-O3"
optFlag _         = "-Og"

defineFlags :: String -> [String]
defineFlags "release"  = ["-DNDEBUG"]
defineFlags "debug"    = ["-g"]
defineFlags otherLevel = error $ "Unknown level '" ++ otherLevel ++ "'"

main :: IO ()
main = shakeArgs shakeOptions $ do
  want ["_build/debug/test", "_build/release/test"]

  phony "clean" $ do
    putNormal "Cleaning _build"
    removeFilesAfter "_build" ["//*"]

  let objs level srcDir = do
        cpps <- getDirectoryFiles (".." </> srcDir) ["//*.cpp"]
        let objs = ["_build" </> level </> "obj" </> srcDir </> c -<.> "o"
                   | c <- cpps]
        need objs
        return objs

  "_build/*/test" %> \out -> do
    let level = takeDirectory1 $ dropDirectory1 out
    objs1 <- objs level "src"
    objs2 <- objs level "tests"
    cmd "g++" [optFlag level] "-Wall -Werror -pthread -o" [out]
        (defineFlags level) objs1 objs2

  "_build/*/obj//*.o" %> \out -> do
    let level = takeDirectory1 $ dropDirectory1 out
    let c = ".." </> dropDirectory1 (dropDirectory1 (dropDirectory1 (out -<.> "cpp")))
        m = out -<.> "m"
    cFlags <- getEnvWithDefault "" "CFLAGS"
    () <- cmd "gcc" [optFlag level] "-Wall -Werror -pthread -c" [c] "-o" out
                (defineFlags level)
                "-I../src/h"
                "-MMD -MF" [m] "-std=c++11" cFlags
    needMakefileDependencies m
