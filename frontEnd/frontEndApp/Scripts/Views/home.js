$(function () {
    // global vars
    var mySqlLoading = false;
    var lambdaLoading = false;

    var realTime = false;

    // Realtime
    $('#realTimeCheckBox').click(function () {
        realTime = this.checked;

        // Update the current view
        if (realTime) {
            afterSetExtremes({
                'min': $('#MySQL').highcharts().xAxis[0].getExtremes().min,
                'max': new Date().getTime()
            });
        }
    });


    /**
    * Load data for a specific time range and sensorID into the MySQL chart
    */
    function loadSensorData(index, buildingID, start, end, first, lambda) {
        return $.getJSON("/api/sensorData/?buildingID=" + buildingID + "&start=" + start + "&end=" + end + "&lambda=" + lambda, function (data) {
            //parse the data
            data = $.parseJSON(data);

            // Null value for extremes
            if (first)
                data = [].concat(data, [[new Date().getTime(), null, null, null, null]]);

            // get the chart
            var chart = lambda ? $('#Lambda').highcharts() : $('#MySQL').highcharts();

            // does this series exist?
            if (chart.get('Building ' + buildingID) == null) {
                // no - add the series
                chart.addSeries({
                    name: 'Building ' + buildingID,
                    id: 'Building ' + buildingID,
                    data: data
                }, true);
            }
            else {
                // yes - replace the data
                chart.get('Building ' + buildingID).setData(data);
            }
        });
    }

    /**
     * Load new data depending on the selected min and max
     */    
    function afterSetExtremes(e) {

        var MySQL = $('#MySQL').highcharts();
        var Lambda = $('#Lambda').highcharts();        

        // If realtime, force them to be current
        if (realTime) {
            // Get current extremes
            var difference = e.max - e.min;
            
            // set new max and min
            e.max = new Date().getTime();
            e.min = e.max - difference;

            // Set extremes
            MySQL.xAxis[0].setExtremes(e.min, e.max, false);
        }
        else {
            // Else let's make it look nicer
            MySQL.showLoading('Loading data from server...');
            Lambda.showLoading('Loading data from server...');
        }

        if (mySqlLoading != true) {
            // set the loading, to prevent this function running concurrently
            mySqlLoading = true;

            // Get the data
            $.getJSON("/api/building", function (buildings) {
                // parse the json
                buildings = $.parseJSON(buildings);

                // array of loading events
                var loads = [];

                // for each building, add the loading event to the array
                $.each(buildings, function (index, building) { loads.push(loadSensorData(index, building.ID, Math.round(e.min), Math.round(e.max), false, false)) })

                // Done loading
                $.when.apply($, loads).done(function () {
                    MySQL.hideLoading();
                    mySqlLoading = false;

                    if(realTime)
                    {
                        //let's do it again!
                        window.setTimeout(afterSetExtremes(e), 1000);
                    }
                })
            });
        };

        if (lambdaLoading != true) {
            // set the loading, to prevent this function running concurrently
            lambdaLoading = true;

            // Get the data
            $.getJSON("/api/building", function (buildings) {
                // parse the json
                buildings = $.parseJSON(buildings);

                // array of loading events
                var loads = [];

                // for each building, add the loading event to the array
                $.each(buildings, function (index, building) { loads.push(loadSensorData(index, building.ID, Math.round(e.min), Math.round(e.max), false, true)) })

                // Done loading
                $.when.apply($, loads).done(function () {
                    Lambda.hideLoading();
                    lambdaLoading = false;

                    if (realTime) {
                        //let's do it again!
                        window.setTimeout(afterSetExtremes(e), 1000);
                    }
                })
            });
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
    $.getJSON("/api/building", function (buildings) {
        // get the chart
        var MySQL  = $('#MySQL').highcharts();
        var Lambda = $('#Lambda').highcharts();

        // UI stuff
        MySQL.showLoading('Loading data from server...');
        Lambda.showLoading('Loading data from server...');
        mySqlLoading  = true;
        lambdaLoading = true;

        // parse the json
        buildings = $.parseJSON(buildings);

        // array of loading events
        var mySqlLoads  = [];
        var lambdaLoads = [];

        // for each building, add the loading event to the array
        $.each(buildings, function (index, building) {
            mySqlLoads.push(loadSensorData(index, building.ID, 0, new Date().getTime(), true, false));
            lambdaLoads.push(loadSensorData(index, building.ID, 0, new Date().getTime(), true, true));
        })

        // Done mySql
        $.when.apply($, mySqlLoads).done(function () {
            MySQL.hideLoading();
            mySqlLoading = false;
        })

        // Done lambda
        $.when.apply($, lambdaLoads).done(function () {
            Lambda.hideLoading();
            lambdaLoading = false;
        })

        // Set extremes
        MySQL.xAxis[0].setExtremes(MySQL.xAxis[0].getExtremes().min, new Date().getTime(), false);
    });
});