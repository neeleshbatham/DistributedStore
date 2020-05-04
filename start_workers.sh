for i in "$@"
do
  python ./server.py $i &
  echo "Starting server at:  $i"
done
