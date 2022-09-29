set(THALLIUM_FOUND TRUE)

if (NOT TARGET thallium::thallium)
    # Include directories
    find_path(MERCURY_INCLUDE_DIRS mercury.h PATH_SUFFIXES include/ NO_CACHE)
    if (NOT IS_DIRECTORY "${MERCURY_INCLUDE_DIRS}")
        set(THALLIUM_FOUND FALSE)
    else ()
	find_path(THALLIUM_INCLUDE_DIRS thallium.hpp PATH_SUFFIXES include/)
        find_path(THALLIUM_LIBRARY_MERCURY_PATH libmercury.so PATH_SUFFIXES lib/)
	find_path(MARGO_INCLUDE_DIRS margo.h PATH_SUFFIXES include/)
	find_path(THALLIUM_LIBRARY_MARGO_PATH libmargo.so PATH_SUFFIXES lib/)
	find_path(ABT_INCLUDE_DIRS abt.h PATH_SUFFIXES include/)
	find_path(THALLIUM_LIBRARY_ABT_PATH libabt.so PATH_SUFFIXES lib/)
	find_path(CEREAL_INCLUDE_DIRS cereal PATH_SUFFIXES include/)
	
	set(THALLIUM_LIBRARY_LD_PRELOAD ${THALLIUM_LIBRARY_MERCURY_PATH}/libmercury.so ${THALLIUM_LIBRARY_MARGO_PATH}/libmargo.so ${THALLIUM_LIBRARY_ABT_PATH}/libabt.so)
	
	set(THALLIUM_LIBRARIES -lmercury -lmercury_util -lmargo -labt)
	set(MERCURY_LIBRARIES -lmercury -lmercury_util)
	set(MARGO_LIBRARIES -lmargo)
	set(ARGO_LIBRARIES -labt)
        set(THALLIUM_DEFINITIONS "")
	include_directories(${MERCURY_INCLUDE_DIRS})
	include_directories(${MARGO_INCLUDE_DIRS})
	include_directories(${ABT_INCLUDE_DIRS})
	include_directories(${CEREAL_INCLUDE_DIRS})
        add_library(thallium INTERFACE)
        add_library(thallium::thallium ALIAS thallium)
        target_include_directories(thallium INTERFACE ${THALLIUM_INCLUDE_DIRS})
	target_link_libraries(thallium INTERFACE -L${THALLIUM_LIBRARY_MERCURY_PATH} ${MERCURY_LIBRARIES} -L${THALLIUM_LIBRARY_MARGO_PATH} ${MARGO_LIBRARIES} -L${THALLIUM_LIBRARY_ABT_PATH} ${ARGO_LIBRARIES})
        target_compile_options(thallium INTERFACE ${THALLIUM_DEFINITIONS})
    endif ()
    include(FindPackageHandleStandardArgs)
    # handle the QUIETLY and REQUIRED arguments and set ortools to TRUE
    # if all listed variables are TRUE
    find_package_handle_standard_args(thallium
            REQUIRED_VARS THALLIUM_FOUND THALLIUM_LIBRARIES THALLIUM_INCLUDE_DIRS)
endif ()
