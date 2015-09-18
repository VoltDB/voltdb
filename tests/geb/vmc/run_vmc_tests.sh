#!/usr/bin/env bash
# Runs the GEB tests of the VMC (VoltDB Management Center); a VoltDB server
# must already be running

# Divide arguments into the -P args (which go first), the -- arguments
# (which go last), and the rest (which go in the middle)
for i in "$@"; do
    if [[ $i == -P* ]]; then
        FIRST_ARGS=("${FIRST_ARGS[@]}" "$i")
    elif [[ $i == --* ]]; then
        LAST_ARGS=("${LAST_ARGS[@]}" "$i")
    # Special case: "debug" is short for "-PdebugPrint=true"
    elif [[ $i == "debug" ]]; then
        FIRST_ARGS=("${FIRST_ARGS[@]}" "-PdebugPrint=true")
    else
        MIDDLE_ARGS=("${MIDDLE_ARGS[@]}" "$i")
    fi
done

# Use default browser, firefox, if none was specified
if [ ${#MIDDLE_ARGS[@]} -le 0 ]; then
    MIDDLE_ARGS=("firefox")
fi

# Make sure the "--rerun-tasks" arg is included, if not specified explicitly
FOUND=false
for item in "${LAST_ARGS[@]}"; do
    if [[ "$item" == "--rerun-tasks" ]]; then
        FOUND=true
        break
    fi
done
if [ $FOUND == false ]; then
    LAST_ARGS=("${LAST_ARGS[@]}" "--rerun-tasks")
fi

# Run the GEB tests of the VMC
echo "Executing:"
echo "./gradlew ${FIRST_ARGS[@]} ${MIDDLE_ARGS[@]} ${LAST_ARGS[@]}"
./gradlew ${FIRST_ARGS[@]} ${MIDDLE_ARGS[@]} ${LAST_ARGS[@]}
