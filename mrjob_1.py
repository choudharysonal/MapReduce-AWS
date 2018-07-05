from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env

class MRJobCount(MRJob):

    class MRJobCount(MRJob):
        def mapper(self, key, value):
            lower_height = int(jobconf_from_env('my.job.lower_height'))
            upper_height = int(jobconf_from_env('my.job.upper_height'))
            zipcode = int(jobconf_from_env('my.job.zipcode'))
            (Zipcode, Height) = value.split(',')
            if lower_height <= int(Height) <= upper_height:
                if int(Zipcode) == int(zipcode):
                    yield "Number of people", 1

        def reducer(self, key, values):
            yield key, sum(values)


if __name__ == '__main__':
    MRJobCount.run()