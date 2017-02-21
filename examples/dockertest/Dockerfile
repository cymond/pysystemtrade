FROM python
MAINTAINER Rob Carver <rob@qoppac.com>
RUN pip3 install pandas
RUN pip3 install pyyaml
RUN pip3 install scipy
RUN pip3 install matplotlib
COPY quality/ /quality/
ENV PYTHONPATH /quality:$PYTHONPATH
CMD [ "python3", "/quality/examples/dockertest/dockertest.py" ]
