function(CUSTOM_PROTOBUF_GENERATE_CPP SRCS HDRS)
    if (NOT ARGN)
        message(SEND_ERROR "Error: CUSTOM_PROTOBUF_GENERATE_CPP() called without any proto files")
        return()
    endif ()

    # Create an include path for each file specified
    foreach (FIL ${ARGN})
        get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
        get_filename_component(ABS_PATH ${ABS_FIL} PATH)
        list(FIND _protobuf_include_path ${ABS_PATH} _contains_already)
        if (${_contains_already} EQUAL -1)
            list(APPEND _protobuf_include_path -I ${ABS_PATH})
        endif ()
    endforeach ()

    set(${SRCS})
    set(${HDRS})
    foreach (FIL ${ARGN})
        get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
        get_filename_component(FIL_WE ${FIL} NAME_WE)

        list(APPEND ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc")
        list(APPEND ${HDRS} "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h")

        execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR})

        add_custom_command(
                OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.cc"
                "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.pb.h"
                COMMAND ${PROTOBUF_PROTOC}
                ARGS --cpp_out ${CMAKE_CURRENT_BINARY_DIR} ${_protobuf_include_path} ${ABS_FIL}
                DEPENDS ${ABS_FIL}
                COMMENT "Running C++ protocol buffer compiler on ${FIL}"
                VERBATIM)
    endforeach ()

    set_source_files_properties(${${SRCS}} ${${HDRS}} PROPERTIES GENERATED TRUE)
    set(${SRCS} ${${SRCS}} PARENT_SCOPE)
    set(${HDRS} ${${HDRS}} PARENT_SCOPE)
endfunction()