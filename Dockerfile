FROM public.ecr.aws/lambda/python:3.12

# Copy requirements.txt first to leverage Docker cache
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Copy all project files
# This is done after pip install to ensure changes in code don't trigger pip install again
COPY . ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler
CMD [ "main.handler" ]
