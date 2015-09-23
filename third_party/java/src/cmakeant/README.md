# cmakeant

Ant task to build cmake projects

Cmakeant provides a simple [ant](http://ant.apache.org/) task to integrate [cmake](http://www.cmake.org/) projects into larger projects built with ant.  It makes it easier to control multiplatform cmake builds from a single ant file, different cmake generators and configurations can be specified based on the operating system executing the build. Ant properties can be passed to into cmake as variables and and cmake variables to passed back to ant as properties.

This task calls cmake to generate the platform specific build files (ide projects or makefiles).  It then uses the generated files to build the project, reading the `CMakeCache.txt` to resolve the platform's appropriate build command.  This enables a single ant task to call cmake on any platform, hiding the platform specific code required to launch the build from ant.

See CmakeDoc for more information.

## Why use cmakeant?

You want to build cross platform c/c++ code with ant. This might be because your c/c++ code is part of a bigger java code base, your continuous build system uses ant or you just find the features in ant useful for controlling your build. Cmakeant simplifies the cross platform issues calling cmake from ant, making your ant file and cmake files easier to write and maintain.

---

# Cmake

## Description
Generates a cmake build, and runs the configured build tool.

This task is used to integrate cmake based projects with a larger ant build.

##Parameters
| Attribute | Description | Required |
| --------- | ----------- | -------- |
| srcdir | Location of the source directory, where the top level CMakeLists.txt file is found. | No, defaults to the current directory. |
| bindir | Location to the binary directory, where the cmake output is written, this can be the same as the srcdir. | No, defaults to the current directory |
| buildtype | The type of the build, Debug, Release, RelWithDebInfo or MinSizeRelComma. See notes on build type. | No |
| cleanfirst | Call the clean target first when building. | No, defaults to false |
| cmakeonly | When set to true, runs cmake but does not execute the build | No, defaults to false |
| target | The target of the project to build | No, defaults to the generated all target |

##Parameters specified as nested elements
###generator
Specify the cmake generator to use for each platform. Child of cmake.

| Attribute | Description | Required |
| --------- | ----------- | -------- |
| bindir | Location to the binary directory, where the cmake output is written, this can be the same as the srcdir. | No, defaults to the top level bindir |
| buildtype | The type of the build, Debug, Release, RelWithDebInfo or MinSizeRelComma. See notes on build type. | No, defaults to the top level buildtype | 
| cleanfirst | Call the clean target first when building. | No, defaults to the top level cleanfirst |
| name | The name of the generator to use, this must be a valid cmake generator. | No, defaults cmake platforms default | 
| platform | The name of the platform this generator is valid for. This is tested against the java property os.name. | No, defaults to support all platforms |  
| target | The target of the project to build | No, defaults to the top level target |
| buildargs | Optional additional arguments passed to the build command, for example "-j 8" to enable make to use 8 cores | No |

###variable
Set cmake variables.  If this is a child of the main cmake task then it the variable is set which ever generator is used.  If this or a child of the generator then it is specific to that generator.

| Attribute | Description | Required |
| --------- | ----------- | -------- | 
| name | The name of the variable to set. | Yes | 
| type | The type of the variable. Possible values STRING, FILEPATH, PATH, BOOL. | No, defaults to STRING | 
| value | The value to set the variable. | Yes |  

###readvar
Read the value of a cmake cache variable to an ant varaiable. Child of cmake

| Attribute | Description | Required | 
| --------- | ----------- | -------- |
| name | The name of the variable to read. | Yes |  
| property | The name of the ant property to set. | Yes | 

##Examples

    <taskdef name="cmake" 
        classname="org.iainhull.ant.CmakeBuilder"/>

    <cmake/>

Define the cmake ant task.  Then run cmake in the current directory, using it as both the source directory and binary directory, then executes the resulting make files or projects.


    <taskdef name="cmake" 
        classname="org.iainhull.ant.CmakeBuilder"/>
  
    <cmake srcdir="${src}"
        bindir="${binary}"
        buildtype="${buildtype}" />

Runs cmake in the `${src}` directory and write the output to the `${binary}` directory, then executes the resulting make files or projects.


    <taskdef name="cmake" 
        classname="org.iainhull.ant.CmakeBuilder"/>
  
    <cmake srcdir="${src}"
        bindir="${binary}"
        buildtype="${buildtype}" > 
        
        <generator name="Visual Studio 8 2005" platform="windows" />
        <generator name="Unix Makefiles" platform="SunOS" buildargs="-j 4">
            <variable name="CMAKE_C_COMPILER" type="FILEPATH" value="/opt/SUNWspro/bin/cc" />
            <variable name="CMAKE_CXX_COMPILER" type="FILEPATH" value="/opt/SUNWspro/bin/CC" />
        </generator>
        <generator name="Unix Makefiles" />
        <readvar name="CMAKE_CXX_COMPILER" property="cxxCompilerPath" />
    </cmake>

Runs cmake in the `${src}` directory and write the output to the `${binary}` directory, then executes the resulting make files or projects. 
  * On the `windows` platform the `Visual Studio 8 2005` generator is used
  * On the `SunOS` (Solaris) platform the `Unix Makefiles` generator is used and the path to the C and C Plus Plus compilers is specified (for example to ensure the Sun Studio compiler is used instead of the GCC compiler).  Also "-j 4" is added to the make command line to use of multiple cpu cores.
  * On all other plaforms the `Unix Makefiles` generator is used.
  * Finally after the build files are generated and the build is complete the value of the CMake variable `CMAKE_CXX_COMPILER` is read from the CMakeCache.txt and used to set the ant property `cxxCompilerPath`, this ant property can now be used later in the ant file.

##Build Type Notes
The buildtype attribute of the cmake and generator tags is used to set the cmake variable CMAKE_BUILD_TYPE.  This is usually one of Debug, Release, RelWithDebInfo or MinSizeRelComma see the Cmake documentation for
the CMAKE_BUILD_TYPE and CMAKE_CONFIGURATION_TYPES for more information.
