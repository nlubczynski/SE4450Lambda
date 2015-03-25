$(function () {
    // global vars
    var mySqlLoading = false;
    var lambdaLoading = false;

    /**
    * Load data for a specific time range and sensorID into the MySQL chart
    */
    function loadMySQLSensorData(index, sensorID, start, end, first) {
        return $.getJSON("/api/sensorData/?sensorID=" + sensorID + "&start=" + start + "&end=" + end, function (data) {
            //parse the data
            data = $.parseJSON(data);

            if (first)
                data = [].concat(data, [[new Date().getTime(), null, null, null, null]]);

            // get the chart
            var MySQL = $('#MySQL').highcharts();

            // does this series exist?
            if (MySQL.get('Sensor ' + sensorID) == null) {
                // no - add the series
                MySQL.addSeries({
                    name: 'Sensor ' + sensorID,
                    id: 'Sensor ' + sensorID,
                    data: data
                }, true);
            }
            else {
                // yes - replace the data
                MySQL.get('Sensor ' + sensorID).setData(data);
            }
        });
    }

    /**
     * Load new data depending on the selected min and max
     */    
    function afterSetExtremes(e) {

        var MySQL = $('#MySQL').highcharts();
        var Lambda = $('#Lambda').highcharts();

        MySQL.showLoading('Loading data from server...');
        Lambda.showLoading('Loading data from server...');


        if (mySqlLoading != true) {
            // set the loading, to prevent this function running concurrently
            mySqlLoading = true;

            // Get the data
            $.getJSON("/api/sensor", function (sensors) {
                // parse the json
                sensors = $.parseJSON(sensors);

                // array of loading events
                var loads = [];

                // for each sensor, add the loading event to the array
                $.each(sensors, function (index, sensor) { loads.push(loadMySQLSensorData(index, sensor.ID, Math.round(e.min), Math.round(e.max), false)) })

                // Done loading
                $.when.apply($, loads).done(function () {
                    MySQL.hideLoading();
                    mySqlLoading = false;
                })
            });
        };

        if (lambdaLoading != true) {
            lambdaLoading = true;
            Lambda.hideLoading();
            lambdaLoading = false;
        }
    }

    // MySql chart
    $('#MySQL').highcharts('StockChart', {
        chart: {
            zoomType: 'x'
        },

        navigator: {
            adaptToUpdatedData: false,
        },

        scrollbar: {
            liveRedraw: false
        },

        title: {
            text: 'MySQL Database'
        },

        rangeSelector: {
            buttons: [{
                type: 'minute',
                count: 1,
                text: '1m'
            }, {
                type: 'hour',
                count: 1,
                text: '1h'
            }, {
                type: 'day',
                count: 1,
                text: '1d'
            }, {
                type: 'month',
                count: 1,
                text: '1m'
            }, {
                type: 'month',
                count: 3,
                text: '3m'
            }, {
                type: 'year',
                count: 1,
                text: '1y'
            }, {
                type: 'all',
                text: 'All'
            }],
            inputEnabled: false, // it supports only days
            selected: 7, // all
            allButtonsEnabled: true
        },

        xAxis: {
            events: {
                afterSetExtremes: afterSetExtremes
            },
            minRange: 3600 * 1000 // one hour
        },
    });  


    // Lambda chart
    $('#Lambda').highcharts('StockChart', {
        chart: {
            zoomType: 'x'
        },

        navigator: {
            adaptToUpdatedData: false,
        },

        scrollbar: {
            liveRedraw: false
        },

        title: {
            text: 'Lambda Architecture'
        },

        rangeSelector: {
            enabled: false
        },

        xAxis: {
            events: {
                afterSetExtremes: afterSetExtremes
            },
            minRange: 3600 * 1000 // one hour
        },

        yAxis: {
            floor: 0
        }
    });

    // Do the first loads
    $.getJSON("/api/sensor", function (sensors) {
        // get the chart
        var MySQL = $('#MySQL').highcharts();

        // UI stuff
        MySQL.showLoading('Loading data from server...');
        mySqlLoading = true;

        // parse the json
        sensors = $.parseJSON(sensors);

        // array of loading events
        var loads = [];

        // for each sensor, add the loading event to the array
        $.each(sensors, function (index, sensor) { loads.push(loadMySQLSensorData(index, sensor.ID, 0, new Date().getTime(), true)) })

        // Done loading
        $.when.apply($, loads).done(function () {
            MySQL.hideLoading();
            mySqlLoading = false;
        })

        // Set extremes
        MySQL.xAxis[0].setExtremes(MySQL.xAxis[0].getExtremes().begin, new Date().getTime(), false);
    });
});