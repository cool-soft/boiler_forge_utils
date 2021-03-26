
from datetime import datetime
import pandas as pd
from dateutil.tz import gettz, UTC


if __name__ == '__main__':
    round_step = 180
    tz = gettz("Asia/Yekaterinburg")

    series = pd.Series([
        datetime(2020, 1, 1, 0, 0, 17, tzinfo=tz),
        datetime(2020, 1, 1, 0, 1, 17, tzinfo=tz),
        datetime(2020, 1, 1, 0, 3, 0, tzinfo=tz),
        datetime(2020, 1, 1, 0, 4, 0, tzinfo=tz),
    ])

    series2 = series.dt.tz_convert(UTC)

    series3 = series2.dt.floor("180s")
    print()
