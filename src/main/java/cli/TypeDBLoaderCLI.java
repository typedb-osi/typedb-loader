/*
 * Copyright (C) 2021 Bayer AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cli;

import loader.TypeDBLoader;
import picocli.CommandLine;

@CommandLine.Command(description = "Welcome to the CLI of TypeDB Loader - your TypeDB data loading tool", name = "TypeDB Loader", version = "1.0.0-alpha", mixinStandardHelpOptions = true)
public class TypeDBLoaderCLI {

    public static void main(String[] args) {
        LoadOptions options = LoadOptions.parse(args);
        options.print();
        TypeDBLoader loader = new TypeDBLoader(options);
        loader.load();
    }

}
