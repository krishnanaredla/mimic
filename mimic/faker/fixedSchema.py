# Author : Krishna
# Date   : 2020-08-20

from mimesis.schema import Field, Schema
from mimesis.enums import Gender
import mimesis.random as rd
from typing import List
import random


class Switcher(object):
    """
    Faker is kind of case implementation to get passed value
    fake data

    Usage: 
    x=Switcher()
    phone = x.faker('telephone',format='+1-###-###-###')
    """

    def faker(
        self,
        argument: str,
        mask: str = None,
        start: float = 1.0,
        end: float = 1000.0,
        precision: int = 3,
        start_date: int =  2000,
        end_date: int  =  2022,
        domains: List[str] = None
    ):
        """Main Function which invokes required value based on the argument passed"""
        self.mask = mask
        self.start = start
        self.end = end
        self.precision = precision
        self.start_date = start_date
        self.end_date = end_date
        self.domains = domains 
        self._ = Field("en")
        method_name = argument
        method = getattr(self, method_name, lambda: "Invalid Function")
        return method()

    def custom_code(self):
        """Generates a string in specified format"""
        return rd.Random().custom_code(mask=self.mask)
    def getNameData(self):
        """Returns gender,full_name,first_name and last_name"""
        sex = random.choice(["male", "female"])
        full_name = self._(
            "full_name", gender=Gender.MALE if sex == "male" else Gender.FEMALE
        )
        first_name = full_name.split(" ")[0]
        last_name = full_name.split(" ")[1]
        return {
            "gender": sex,
            "full_name": full_name,
            "first_name": first_name,
            "last_name": last_name,
        }

    def address(self):
        """Returns Address"""
        return self._("address")

    def phone(self):
        """Return a random generated phone number, use format = +#-###-###-####
        to generate in a specific format
        """
        return self._("telephone", mask=self.mask)

    def email(self):
        """"Returns a random generated mail id"""
        return self._("email",domains=self.domains)

    def numbers(self):
        """Return a random number"""
        return self._("integer_number",start=int(self.start),end=int(self.end))

    def decimals(self):
        """
        Returns a decimal value randomly from 1 to 1000 with precision upto 3,
        this can be changed by changing start,end and precision accordingly
        """
        return self._(
            "float_number", start=self.start, end=self.end, precision=self.precision
        )
    
    def city(self):
        """Returns a random city"""
        return self._("city")
    def country(self):
        """Returns a random country"""
        return self._("country")
    def state(self):
        """Returns a random state"""
        return self._("state")
    def zip_code(self):
        """Returns a random zip code"""
        return self._("zip_code")
    def randomDate(self):
        """Returns a random date"""
        return self._("datetime",start=self.start_date,end=self.end_date)
    def randomDay(self):
        """Return a random day"""
        return self._("day_of_month")
    def randomMonth(self):
        """Returns a random month name"""
        return self._("month")
    def randomYear(self):
        """Returns a random year"""
        return self._("year")
    def randomTimestamp(self):
        """Returns a random timestamp"""
        return self._("timestamp")
    