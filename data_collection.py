import requests
import json
import time
import os
import pandas as pd
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO


YEAR_URLS = {
    2025 : "https://dis-prod-awardsearch.s3.amazonaws.com/2025.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=179fca148e6ded18dc6c1ed04bca23036354c03fc596f8e7d6f109d6b5854522",
    2024 : "https://dis-prod-awardsearch.s3.amazonaws.com/2024.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=a459347edb62305dfee9cb0ea76a2f567b8e1337bb0df48f831f2c2838c10780",
    2023 : "https://dis-prod-awardsearch.s3.amazonaws.com/2023.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=91ad0df8b00affad95e8b178768cff38165bd323f4d66266c8eaf6ceaf9f949b",
    2022 : "https://dis-prod-awardsearch.s3.amazonaws.com/2022.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=e4eafe9b27b54ddb500ea6870d01d9a14e1a75fb9d9d6940ad794fe101ebb900",
    2021 : "https://dis-prod-awardsearch.s3.amazonaws.com/2021.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=aa9ad35a7a04fb2beb595f49f2df5bf933e34f7fdc13d208b5f91da111d41bfb",
    2020 : "https://dis-prod-awardsearch.s3.amazonaws.com/2020.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=ac2ccd2e9a06b76836af995619122a344ec5e812546990cc4e486255cbc30c69",
    2019 : "https://dis-prod-awardsearch.s3.amazonaws.com/2019.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=8b03e7885de2f4a42558d3f8607e3b7a8192e7789180e15ea748a5cbac51b3a3",
    2018 : "https://dis-prod-awardsearch.s3.amazonaws.com/2018.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=f2f3883238bfe0b837b23e17686a20ed5e0e8a476ffb805923b4413b806ef1e6",
    2017: "http://dis-prod-awardsearch.s3.amazonaws.com/2017.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=bdb2c4b8409d50f0afc977bacccb840a7eaa03577cb6b53cac8d797c283e4758",
    2016 : "https://dis-prod-awardsearch.s3.amazonaws.com/2016.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=4c489fa7ddf461b46f38429cf3e8cb87f46ff338ea9b1272d8aff43660aa3afa",
    2015 : "https://dis-prod-awardsearch.s3.amazonaws.com/2015.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=ff442b862b65c1755fb8dae8a7c30d11aafd77ad077e7e5c6a23d71683c9206c",
    2014 : "https://dis-prod-awardsearch.s3.amazonaws.com/2014.zip?X-Amz-Security-Token=IQoJb3JpZ2luX2VjEMn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIFpwwwtXUe7yRqu0H8DUwwiETk6jaqvhRrd9STe0rQ6wAiEAiL6eaLpOq6R%2B17Ch%2FLhxkIqiM2zCYBd5AojJR9nP28wqxAUIkf%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARAEGgw2NzM0OTM1NjMyMzUiDDIHJRGnc3kP6FfC7CqYBesyjIkLxR5quy4oJi%2Fkqm%2B1zHeGqgh7GjHvGKWKYskHif9zoFPtGDd%2BO1SvmCbnuy%2FJeKGCzG7%2FNCNjpyVI8QWZUr9QiaVvfYkGONxnEXwQD3PGQwsnZxHV8M7wf1JM7hzsnhWGQJuRullX4fRvScroC6ckqmQjuahVbKAmwvY7H3WqDuf%2FuY8tFjHsu0T%2BQMrIyD%2FmRmsY2e72FiN1HVeA4SXuNx50pVVkdNHOzCPcNI5RHTyK6p7BB66eQSBgQPJCgxW%2FL1FsfsRRLpLnITfCqhbB5a2r75gV7%2BnVEYfGoWYW%2FRCbqsc3jw%2FvCou5M%2BysyG5szokPnuANOv65smVYWkVurKbFe8CbUHaiE8N9Vybq%2F7nS7ptYMg2A9kXh35qLmgUumD2mZGcfCQE0t3slGHXFAK%2BetTin%2BZNACmBxy%2F0V3Fw0bEjbAJN75yvaj2o7bgt83oh0pjBLDgiQtJGya%2BwjCE5ek7IvMeJ3w9La1Avlbb5ZQBxpLhDZC0uX0CHw3pIJWtEfNd9zwwHiXZzVbBD%2Bj9WqQmw%2FYm%2FRVu1tfHIDdPLx7j%2ByWcjMyI6LAo4D9KxvPcoUVCqgHDMqRRxiEp4J657OzL64IqoajJI1%2B10VVr8Eq6%2Bm7ce%2FdWa11A6shAdwr4nHADOPJ1lSf1HMHplMTTPamkwkrOgqpNzHc3jMl3MBMf8jmv5mUNRsdQO%2FZRwILfySb%2FTUKzH5A9S6bEU1RhfYBxPPymGWztsHOU2Z9sT%2F0I7kapRITX%2BjqMu7%2F%2B2ij1EV%2BCGEAxjqOzBgx3IIp1m3PfphpxLXKsxeXGur0Q6HimX9P1EjJm%2FQuwRZnkXPVxtiVg1zQ8mIvywlIUTKKRreOyTG6ITnyzeI4ixyRg5kUvQw2dHWyQY6sQHZ3G3kSeKkee%2F3o0%2BjWFF7jpNt82AQ2peibeKBzN4q%2F%2FCPqvIz2t5OD8vQKPcB3szlJTvuMpBTOjak0DgFwqFTkZTZeVtDD736Bx8sktH73enqoDu6RWoGxS%2B33MRqMUV2m25TLpRuaOujC7k0Z8LP85pCmSPibAzb%2F1UU4M7QON%2F2AcEFcfqQ2y%2F%2FMeFwfSOPTt%2FmRW2P05fQDyfHJnWJIqhm9zve6mhj4HpwdlTLBAc%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20251207T201250Z&X-Amz-SignedHeaders=host&X-Amz-Credential=ASIAZZT2YG5R4PAWMOBO%2F20251207%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Expires=86400&X-Amz-Signature=b7eb220ef974b5b61ab05d6f002443cd9a41153509334dd8f26acba74f6a2ad9",
}


MAX_RETRIES = 3
TIMEOUT = 120
DEFAULT_PAUSE = 0.2


def download_year_zip(year: int) -> bytes | None:
    url = YEAR_URLS.get(year)
    if url is None:
        print(f"⚠️  No URL configured for year {year}. Skipping.")
        return None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"⬇️  Downloading ZIP for year {year} (attempt {attempt})")
            r = requests.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return r.content
        except Exception as e:
            print(f"⚠️  [Year {year} attempt {attempt}] {e}")
            time.sleep(2 * attempt)

    print(f"❌  Skipping year {year} after {MAX_RETRIES} retries.")
    return None


def get_time_data(
    start_year: int,
    end_year: int,
    output_dir,
    pause: float = DEFAULT_PAUSE,
    overwrite: bool = False,
) -> int:
    if end_year < start_year:
        raise ValueError("end_year must be >= start_year")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    available_years = sorted(
        y for y in YEAR_URLS.keys() if start_year <= y <= end_year
    )

    if not available_years:
        raise ValueError(
            f"No years in YEAR_URLS fall within the range {start_year}-{end_year}."
        )

    total_awards = 0

    try:
        for year in available_years:
            year_file = output_dir / f"{year}.json"

            if overwrite and year_file.exists():
                os.remove(year_file)
                print(f"🧹 Deleted old file for year {year}: {year_file}")

            print(f"\n📆 Processing year {year}")

            zip_bytes = download_year_zip(year)
            if zip_bytes is None:
                print(f"✅ Finished / skipped year {year} (no ZIP).")
                continue

            awards = []

            try:
                with ZipFile(BytesIO(zip_bytes)) as zf:
                    names = zf.namelist()
                    json_files = [n for n in names if n.lower().endswith(".json")]

                    if not json_files:
                        print(f"⚠️  No JSON files found inside ZIP for year {year}.")
                        continue

                    for name in json_files:
                        try:
                            raw = zf.read(name).decode("utf-8")
                            data = json.loads(raw)

                            if isinstance(data, list):
                                awards.extend(data)
                            else:
                                awards.append(data)
                        except Exception as e:
                            print(f"⚠️  Error parsing {name} in year {year}: {e}")

            except Exception as e:
                print(f"⚠️  Error opening ZIP for year {year}: {e}")
                continue

            if not awards:
                print(f"✅ Finished year {year} (no awards parsed).")
                continue

            with year_file.open("w", encoding="utf-8") as f_out:
                json.dump(awards, f_out, ensure_ascii=False)

            count = len(awards)
            total_awards += count
            print(f"   ✓ Saved {count} awards for year {year} → {year_file}")
            time.sleep(pause)

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user. Stopping downloads gracefully.")

    print("\nCompleted.")
    print(f"Total awards saved across all years: {total_awards}")
    print(f"Directory: {output_dir}")

    return total_awards


def read_terminated(file_path):
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    nsf_terminations = pd.read_csv(
        "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-05-06/nsf_terminations.csv"
    )
    nsf_terminations.to_csv(file_path, index=False)


def download_legislators(file_path):
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    url = "https://unitedstates.github.io/congress-legislators/legislators-historical.json"
    print("⬇️  Downloading legislators JSON")
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    with file_path.open("w", encoding="utf-8") as f_out:
        json.dump(data, f_out, ensure_ascii=False)
    print(f"   ✓ Legislators saved to {file_path}")


def download_cruz_list(file_path):
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    url = "https://www.commerce.senate.gov/index.cfm?a=files.serve&File_id=94060590-F32F-4944-8810-300E6766B1D6"
    print("⬇️  Downloading Cruz list (Excel XML)")
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    content = r.content

    from io import BytesIO

    df = pd.read_excel(BytesIO(content))
    df.to_csv(file_path, index=False)
    print(f"   ✓ Cruz list saved to {file_path}")


if __name__ == "__main__":
    BASE_DIR = Path(".")
    DATALAKE_DIR = BASE_DIR / "datalake"
    LANDING_DIR = DATALAKE_DIR / "landing"
    NSF_DIR = LANDING_DIR / "nsf_grants"

    PATH_LEGISLATORS = LANDING_DIR / "legislators.json"
    PATH_CANCELLED = LANDING_DIR / "cruz_list.xlsx"
    PATH_TERMINATED_DATA = LANDING_DIR / "terminated_data.csv"

    min_year = min(YEAR_URLS.keys())
    max_year = max(YEAR_URLS.keys())


    read_terminated(PATH_TERMINATED_DATA)
    print("Terminated data downloaded")

    download_legislators(PATH_LEGISLATORS)
    print("downloaded legislators")

    download_cruz_list(PATH_CANCELLED)
    print("downloaded cruz list")

    total = get_time_data(
        start_year=min_year,
        end_year=max_year,
        output_dir=NSF_DIR,
        overwrite=True,
    )
    print(f"Total awards downloaded: {total}")
