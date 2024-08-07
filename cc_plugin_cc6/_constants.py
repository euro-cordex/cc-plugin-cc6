from datetime import timedelta

# Definition of maximum deviations from the given frequency
deltdic = {}
deltdic["monmax"] = timedelta(days=31.5).total_seconds()
deltdic["monmin"] = timedelta(days=27.5).total_seconds()
deltdic["mon"] = timedelta(days=31).total_seconds()
deltdic["daymax"] = timedelta(days=1.1).total_seconds()
deltdic["daymin"] = timedelta(days=0.9).total_seconds()
deltdic["day"] = timedelta(days=1).total_seconds()
deltdic["1hrmin"] = timedelta(hours=0.9).total_seconds()
deltdic["1hrmax"] = timedelta(hours=1.1).total_seconds()
deltdic["1hr"] = timedelta(hours=1).total_seconds()
deltdic["3hrmin"] = timedelta(hours=2.9).total_seconds()
deltdic["3hrmax"] = timedelta(hours=3.1).total_seconds()
deltdic["3hr"] = timedelta(hours=3).total_seconds()
deltdic["6hrmin"] = timedelta(hours=5.9).total_seconds()
deltdic["6hrmax"] = timedelta(hours=6.1).total_seconds()
deltdic["6hr"] = timedelta(hours=6).total_seconds()
deltdic["yrmax"] = timedelta(days=366.1).total_seconds()
deltdic["yrmin"] = timedelta(days=359.9).total_seconds()
deltdic["yr"] = timedelta(days=360).total_seconds()
