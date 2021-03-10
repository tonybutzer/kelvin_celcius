FROM python

RUN pip install xarray \
    && pip install boto3 \
    && pip install rioxarray \ 
    && pip install rasterio


RUN mkdir /home/kelvin


COPY convert_kelvin_celsius.py /home/kelvin


WORKDIR /home/kelvin
