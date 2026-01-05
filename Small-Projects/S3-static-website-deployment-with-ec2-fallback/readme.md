This CloudFormation template creates an S3 bucket configured for static website hosting and applies a bucket policy for public read access. 
It also uses S3 routing rules to redirect 404 errors to an EC2 instance acting as a fallback.
Can be expanded to handle multiple errors/request redirection if needed.

To do:
- make it parameterized with mappings, parameters and conditions as needed.
