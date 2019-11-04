# DataFed Python Utilities
Helper functions and classes to work with ORNL's [DataFed](https://datafed.ornl.gov/)

Note:
1. These are NOT officially provided functions or utilities with the DataFed package.
2. These are helper functions I put together to support my specific applications.
3. These are ad-hoc functions that can change quite a bit and rapidly
4. These functions are not well documented, but should be rather straightforward to understand, use, fix, and build upon. 

To get started:
1. Install Globus Personal Connect if you are using a personal computer and not a shared resource like a cluster
2. Write down the Endpoint ID
3. Install the DataFed Command Line Interface (CLI) client via:
   ``pip install datafed``
4. In a terminal window - type: ``datafed setup``. Follow the instructions and provide your DataFed ID and password
5. You can now start using DataFed using the 
