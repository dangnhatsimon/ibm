#!/bin/bash

echo "Are you looking for new position?"
echo "Enter \"y\" for yes, \"n\" for no."
echo -n ""
read response

if [ $response == "y" ]
then
    echo "I'm pleased to hear you are looking for new role!"
elif [ $response == "n" ]
then 
    echo "Oh! I'm sorry for annoying you!"
else
    echo "Your response must be either 'y' or 'n'. Re-run the script"
fi
