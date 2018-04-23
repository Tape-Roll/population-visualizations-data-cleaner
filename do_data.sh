for j in "-s", "-c"
do
    for i in "09", 10, 11, 12, 13, 14, 15, 16
    do
        node "index.js" "$j" "20$i"
        wait $(jobs -p)
    done
done

echo "Yay!"