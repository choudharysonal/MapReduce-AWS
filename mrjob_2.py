from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env

class MRJobCount(MRJob):

    def mapper(self, _, line):
        lower = int(jobconf_from_env('my.job.lower_height'))
        upper = int(jobconf_from_env('my.job.upper_height'))
        zipc = int(jobconf_from_env('my.job.zip_code'))

        if lower > upper:
            lower, upper = upper, lower

        (GivenName,ZipCode,Centimeters) = line.split(',')

        if lower <= int(Centimeters) <= upper:
            if int(ZipCode) == zipc:
                yield "row", (GivenName, ZipCode, Centimeters)

    def reducer(self, key, values):
        tuple_list = list()
        if key == "row":
            tuple_list.append([val for val in values])
        yield key, tuple_list


if __name__ == '__main__':
    MRJobCount.run()