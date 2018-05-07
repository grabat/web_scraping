if [ -e /root/webscraping/lambda_function.zip ]; then
  rm /root/webscraping/lambda_function.zip
fi
cd /root/webscraping/lib/python3.6/site-packages && zip -r /root/webscraping/lambda_function.zip .
cd /root/webscraping && zip -g lambda_function.zip lambda_function.py
zip -g lambda_function.zip settings.py
