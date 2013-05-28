/*
 * Copyright 2007 Kasper B. Graversen
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Provides <tt>CellProcessor</tt> classes for enforcing constraints.
 * <p>
 * Note however, that in order for these processors to carry out their constraint logic, they may convert the input
 * data. For example, the <tt>Strlen</tt> constraint, given the number 17, converts it to the string <tt>"17"</tt> before doing its length
 * check.
 */
package org.supercsv.cellprocessor.constraint;