for entry in ./*
do
if [ ${entry:(-3)} == "dot" ] 
then
  echo "$entry"
  neato -T png $entry > $entry+".png"
fi
done
