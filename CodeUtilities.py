class CodeUtilities:

    def __init__(self, pd):
        usc_pd = pd

    def getStateCode(iso_region):
        """
        get state code from string with pattern US-MD
        :param iso_region: string like US-MD
        :return: second part of string -> state code
        """
        try:
            result = iso_region.split("-")[1]
        except Exception:
            result = 'XX'
        return result

    def getCityCode(self, s, sc):
        """
        :param s: string for city name
        :param sc: string for state code
        :return: String 3 letter city code
        """
        try:
            result = \
                self.usc_pd.loc[(self.usc_pd['name'] == s.upper()) & (self.usc_pd['state_code'] == sc)]['code'].iloc[0]
        except Exception:
            result = '000'
        return result
