#!/usr/bin/env bash

# This test script attempts to reproduce a memory fragmentation issue that
# we have observed running KVM in CentOS 6. Using the default values of the
# configuration constants, this simulates the following behavior:
# 1. Preload 2000 64KB chunks, memset entirely to 0, which live for the
#    entire duration of the test
# 2. For half the configured test duration, allocate/deallocate in a
#    randomized fashion a number of 256KB buffers. Only 4-8KB of these
#    buffers will be "used," simulated by a memset to 0, and a maximum
#    of 5000 such allocations will be outstanding at any given time
# 3. After half of the configured duration, deallocate any buffers outstanding
#    from step 2.
# 4. For the second half of the configured duration, do the following: every
#    5ms, allocate and memset to 0 a chunk of between 40 and 200 bytes, then
#    deallocate it after 100 microseconds
# 5. Deallocate the long-lived buffers from step 1.
# We should not expect to see any memory growth during step 4 above. If any
# is observed, that should be taken as a reproduction of the memory issue
# mentioned above. Configuration may be achieved by setting the environment
# variables specified below.

VARS=""
##############################################################################
# Settings for the partially filled buffers during the load phase
##############################################################################
# KVMF_BUFFER_SIZE [bytes; default 256KB]
#    Allocations will be a deterministic size
if [ -n "$KVMF_BUFFER_SIZE" ]; then
    VARS="$VARS -DBUFFER_SIZE=$KVMF_BUFFER_SIZE"
fi

# KVMF_MIN_BUFFER_FILL [bytes; default 4KB]
# KVMF_MAX_BUFFER_FILL [bytes; default 8KB]
#    Simulate partially filling these buffers by memsetting a uniformly
#    distributed chunk at the beginning of the buffer to 0
if [ -n "$KVMF_MIN_BUFFER_FILL" ]; then
    VARS="$VARS -DMIN_BUFFER_FILL=$KVMF_MIN_BUFFER_FILL"
fi
if [ -n "$KVMF_MAX_BUFFER_FILL" ]; then
    VARS="$VARS -DMAX_BUFFER_FILL=$KVMF_MAX_BUFFER_FILL"
fi

# KVMF_MAX_LIVE_BUFFERS [default 5000]
#    Maximum number of buffers that can be allocated at any given time during
#    the load phase
if [ -n "$KVMF_MAX_LIVE_BUFFERS" ]; then
    VARS="$VARS -DMAX_LIVE_BUFFERS=$KVMF_MAX_LIVE_BUFFERS"
fi

##############################################################################
# Settings for the static preloaded chunks that live through the whole test
##############################################################################
# KVMF_PRELOAD_CHUNKS [default 2000]
#    Number of buffers to preload
if [ -n "$KVMF_PRELOAD_CHUNKS" ]; then
    VARS="$VARS -DPRELOAD_CHUNKS=$KVMF_PRELOAD_CHUNKS"
fi

# KVMF_PRELOAD_CHUNK_SIZE [bytes; default 64KB]
#    Size of each preload chunk
if [ -n "$KVMF_PRELOAD_CHUNK_SIZE" ]; then
    VARS="$VARS -DPRELOAD_CHUNK_SIZE=$KVMF_PRELOAD_CHUNK_SIZE"
fi

##############################################################################
# Settings for the periodic allocations during the quiesced period
##############################################################################
# KVMF_PERIODIC_CHUNK_MIN_SIZE [bytes; default 40]
# KVMF_PERIODIC_CHUNK_MAX_SIZE [bytes; default 80]
#    Periodic allocations will be uniformly distributed over the above range
#    and memset to 0
if [ -n "$KVMF_PERIODIC_CHUNK_MIN_SIZE" ]; then
    VARS="$VARS -DPERIODIC_CHUNK_MIN_SIZE=$KVMF_PERIODIC_CHUNK_MIN_SIZE"
fi
if [ -n "$KVMF_PERIODIC_CHUNK_MAX_SIZE" ]; then
    VARS="$VARS -DPERIODIC_CHUNK_MAX_SIZE=$KVMF_PERIODIC_CHUNK_MAX_SIZE"
fi

# KVMF_PERIODIC_CHUNK_LIFESPAN_MICROS [microseconds; default 100]
#    Length of time before freeing this allocation. Must be between 0 and 5000
if [ -n "$KVMF_PERIODIC_CHUNK_LIFESPAN_MICROS" ]; then
    VARS="$VARS -DPERIODIC_CHUNK_LIFESPAN_MICROS=$KVMF_PERIODIC_CHUNK_LIFESPAN_MICROS"
fi

##############################################################################
# Global test settings
##############################################################################
# KVMF_APPROX_RUNTIME [seconds; default 5 minutes]
#    Target runtime for the test, determinted by the number of 5ms sleeps
if [ -n "$KVMF_APPROX_RUNTIME" ]; then
    VARS="$VARS -DAPPROX_RUNTIME=$KVMF_APPROX_RUNTIME"
fi

# KVMF_APPROX_STATS_INTERVAL [seconds; default 5]
#    Target interval for printing progress to stdout
if [ -n "$KVMF_APPROX_STATS_INTERVAL" ]; then
    VARS="$VARS -DAPPROX_STATS_SECONDS=$KVMF_APPROX_STATS_INTERVAL"
fi

gcc -o kvm_fragmentation_test $VARS fragmentation.c
./kvm_fragmentation_test