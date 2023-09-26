#!/bin/bash

echo -n "Input the integer n1= "
read n1
echo -n "Input the integer n2= "
read n2
sum=$(($n1+$n2))
pro=$(($n1*$n2))
echo "n1+n2= $sum"
echo "n1*n2= $pro"

if [ $sum -gt $pro ]
then
    echo "The sum is greater than the product"
elif [[ $sum == $pro ]]
then
    echo "The sum is equal to the product"
elif [ $sum -lt $pro ]
then
    echo "The sum is less than the product"
fi