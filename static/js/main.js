function sendDate() {
    let dateValue = $('#date').val();
    $.post("/shinbus", { date: dateValue }, function (response) {
        console.log("Selected date:", response);
        // updatePlates(response);

        // Update the DataTable with the new data
        var table = $('#datatable').DataTable();
        table.clear(); // Clear the existing data
        table.rows.add(response); // Add the new data
        table.draw(); // Redraw the table
    });
}

$('#date').change(sendDate);
$(document).ready(function () {
    var plateData = {{ plates_data | tojson | safe }};
$('#datatable').DataTable({
    data: plateData,
    columns: [
        // {data: 'Time' },
        // {data: 'Company' },
        { data: '車牌號碼' },
        { data: 'hours' }
    ],
    pageLength: 10 // 每頁限制筆數
});

// Variables
var rawData = [];
// var chart;
var chartData1 = {
    labels: [],
    datasets: [
        {
            label: 'ActDCCur',
            data: [],
            borderColor: 'rgb(75, 192, 192)',
            fill: false,
            yAxisID: 'yActDCCur'
        },
        {
            label: 'ActDCVolt',
            data: [],
            borderColor: 'rgb(255, 99, 132)',
            fill: false,
            yAxisID: 'yActDCVolt'
        }
    ]
};
var chartData2 = {
    labels: [],
    datasets: [
        {
            label: 'ActDCCur vs ActDCVolt',
            data: [],
            borderColor: 'rgb(75, 192, 192)',
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            pointRadius: 5,
            fill: false
        },
        // {
        //     label: 'ActDCVolt',
        //     data: [],
        //     borderColor: 'rgb(255, 99, 132)',
        //     fill: false
        // }
    ]
};
var chartData3 = {
    datasets: [{
        label: 'ActDCCur vs ActDCVolt',
        data: [],
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        pointRadius: 5
    }]
};

function fetchDataByPlate() {
    var selectedCompany = $("#companySelect").val();
    var selectedBusPlate = $("#plateSelect").val();
    var selectedDate = $('#date').val();

    if (selectedBusPlate) {
        let url = `/query-plate/${selectedDate}/${selectedBusPlate}`;
        fetch(url)
            .then(response => response.json())
            .then(data => {
                rawData = data;
                updateChartData();
                // chart.update();
                // updateChartData2();
                // chart2.update();
            })
            .catch(error => {
                console.error('There was an error fetching the data:', error);
            });
    }
}

function fetchDataByHour() {
    var selectedBusPlate = $("#plateSelect").val();
    var selectedHour = $("#hourSelect").val();

    if (selectedBusPlate && selectedHour !== "Hour") {
        let url = `/query-hour/${selectedDate}/${selectedBusPlate}/${selectedHour}`;
        // let url = `/query-hour/${selectedBusPlate}/${selectedHour}`;
        fetch(url)
            .then(response => response.json())
            .then(data => {
                rawData = data;  // 您原本的程式碼將資料存放在 rawData

                // 以下為新加入的程式碼，用於更新 chart3 的資料
                updateChartData3(data);
                chart3.update();
            })
            .catch(error => {
                console.error('There was an error fetching the data:', error);
            });
    }
}

function updateChartBasedOnSelection() {
    var selectedCompany = $("#companySelect").val();
    var selectedBusPlate = $("#plateSelect").val();
    var selectedHour = $("#hourSelect").val();

    if (selectedBusPlate && selectedHour !== "Hour") {
        fetchDataAndUpdateChart(selectedCompany, selectedBusPlate);
    }
}
$("#datatable").on("click", ".plate-link", function (event) {
    event.preventDefault(); // 阻止預設的跳轉行為

    let link = $(this).attr('href');
    $.get(link, function (response) {
        // let data = JSON.parse(responseData);
        console.log(response);
        rawData = response;
        updateChartData(response);
        // chart.update();
    });
});

function updateChartData(data) {
    // 打印輸入的數據以進行檢查
    console.log("Data received:", data);

    // 如果 data 不是陣列，直接返回
    if (typeof data === 'string') {
        try {
            data = JSON.parse(data);
        } catch (e) {
            console.error("Failed to parse data:", e);
            return;
        }
    }

    // 更新 chartData1 和 chart1
    chartData1.labels = data.map(item => new Date(item.Time).toLocaleTimeString());
    chartData1.datasets[0].data = data.map(item => item.ActDCCur);
    chartData1.datasets[1].data = data.map(item => item.ActDCVolt);
    chart.update();

    // 更新 chartData3 和 chart3
    chartData3.datasets[0].data = data.map(item => ({
        x: item.ActDCCur,
        y: item.ActDCVolt
    }));
    chart3.update();
}

function updateChartData3(data) {
    chartData3.datasets[0].data = data.map(item => ({
        x: item.ActDCCur,
        y: item.ActDCVolt
    }));
    chart3.update();
}

$("#plateQueryButton").click(fetchDataByPlate);
$("#hourQueryButton").click(fetchDataByHour);

// Initialize Chart.js
var ctx1 = $('#lineChart')[0].getContext('2d');
chart = new Chart(ctx1, {
    type: 'line',
    data: chartData1,
    options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            'yActDCCur': {
                position: 'left',
                beginAtZero: true
            },
            'yActDCVolt': {
                type: 'linear',
                position: 'right',
                beginAtZero: true,
                grid: {
                    drawOnChartArea: false,
                }
            }
        }
    }
});
var ctx2 = $('#secondLineChart')[0].getContext('2d');
chart2 = new Chart(ctx2, {
    type: 'scatter',
    data: chartData2,
    options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            x: {
                type: 'linear',
                position: 'bottom',
                beginAtZero: true
            },
            y: {
                position: 'left', // 指定這是左側的y軸
                beginAtZero: true
            },
            y1: {
                type: 'linear',
                position: 'right',  // 指定這是右側的y軸
                beginAtZero: true,
                grid: {
                    drawOnChartArea: false, // 這將確保只有左邊的y軸有格線
                }
            }
        }
    }
});
var ctx3 = $('#thirdLineChart')[0].getContext('2d');
chart3 = new Chart(ctx3, {
    type: 'scatter',  // 使用散佈圖
    data: chartData3,
    options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
            x: {
                type: 'linear',
                position: 'bottom',
                beginAtZero: true,
                title: {
                    display: true,
                    text: 'ActDCCur'
                }
            },
            y: {
                type: 'linear',
                position: 'left',
                beginAtZero: true,
                title: {
                    display: true,
                    text: 'ActDCVolt'
                }
            }
        }
    }
});

$("#plateSelect").change(function () {
    var selectedBusPlate = $(this).val();
    var selectedDate = $('#date').val(); // 获取当前选择的日期
    if (selectedBusPlate) {
        $.get(`/get-hours/${selectedDate}/${selectedBusPlate}`, function (data) {
            $("#hourSelect").empty().append('<option selected>Hour</option>');
            data.forEach(function (hour) {
                $("#hourSelect").append(`<option value="${hour}">${hour}</option>`);
            });
        });
    }
});
		});