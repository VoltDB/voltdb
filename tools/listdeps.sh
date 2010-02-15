echo "\n** storage **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" storage/*  | sort |uniq

echo "\n** execution **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" execution/*  | sort |uniq

echo "\n** plannodes **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" plannodes/*  | sort |uniq

echo "\n** stats **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" stats/*  | sort |uniq

echo "\n** executors **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" executors/*  | sort |uniq

echo "\n** logging **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" logging/*  | sort |uniq

echo "\n** common **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" common/*  | sort |uniq

echo "\n** indexes **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" indexes/*  | sort |uniq

echo "\n** expressions **"
egrep -o -h "#include.*?(execution|executors|expressions|indexes|plannodes|stats|storage)/" expressions/*  | sort |uniq
