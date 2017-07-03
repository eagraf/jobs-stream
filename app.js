const csv = require('csvtojson');
const RTM = require('satori-sdk-js');
const scraperjs = require('scraperjs');

const config = require('./config');

// Array of every U.S. zip code, also stores the last job posted in that area.
this.zips = [];

// Set up satori connection
var roleSecretProvider = RTM.roleSecretAuthProvider(config.role, config.roleSecretKey);

var rtm = new RTM(config.endpoint, config.appkey, {
    authProvider: roleSecretProvider
});

rtm.start();

// client enters 'connected' state
rtm.on("enter-connected", function() {
    // Load zip csv after satori connection established
    load();
});

//Load up the csv file
var load = () => {

    console.log(this.zips); 
    csv()
    .fromFile(config.csvFilePath)
    .on('end_parsed', (json) => {
        console.log("Finally Done!");
        
        // Start filling in the last job for each zip code.
        getLast(json, 0);
    });
};

// Get the last job posted for a given zip code
var  getLast = (data, index) => {
    // Url from indeed.com
    // Specifies only jobs for that zip, sorted by date
    var url = 'https://www.indeed.com/jobs?as_and=&as_phr=&as_any=&as_not=&as_ttl=&as_cmp=&jt=all&st=&salary=&radius=0&l=' + data[index].zipcode + '&fromage=any&limit=1&sort=date&psf=advsrch';
    scraperjs.StaticScraper.create(url)
    .scrape(($) => {

        job = {};

        // Select the 'boxes', enclosing divs for job postings
        var boxes = $('.row.result');

        // Scrape job name
        job.name = $(boxes[0]).find('a.turnstileLink').text().trim();

        // Scrape company name
        job.company = $(boxes[0]).find('.company>span>a').text().trim();
        if (!job.company) job.company = $(boxes[0]).find('.company').text().trim();

        // Scrape posted location
        job.location = $(boxes[0]).find('.location>span').text().trim();
        if (!job.location) job.location = $(boxes[0]).find('span.location').text().trim();

        // Scrape job posting url
        var href = $(boxes[0]).find('a.turnstileLink').attr('href');
        job.url = 'https://www.indeed.com' + href;

        var zip = {
            geo: data[index],
            job: job
        };

        // Publish message to satori
        publishJob(job, data[index]);
        console.log(zip.geo.placename);
        
        this.zips.push(zip);

    }).then(() => {
        // Get the last job posting for the next zip code
        if (index + 1 < data.length) getLast(data, index + 1);
        else {
            console.log("Finished sraping initial offerings.");
            console.log(this.zips[0]);

            // Begin checking for updates at each zip code
            check(index);
        }
    });
};

// Check if new job postings have been made in a zip
var check = (index) => {

    // Poll indeed.com 
    // Specify only jobs in that zip, sorted by date, with a limit of 100 responses
    var url = 'https://www.indeed.com/jobs?as_and=&as_phr=&as_any=&as_not=&as_ttl=&as_cmp=&jt=all&st=&salary=&radius=0&l=' + this.zips[index].geo.zipcode + '&fromage=any&limit=100&sort=date&psf=advsrch';
    scraperjs.StaticScraper.create(url)
    .scrape(($) => {

        var boxes = $('.row.result');

        var curJob = this.zips[index].job;
        var lastJob = {};
       
        // For each job, if it does not equal the job received from the last poll, send satori message
        for (var i = 0; i < boxes.length; i++) {

            var job = {};

            job.name = $(boxes[i]).find('a.turnstileLink').text().trim();

            job.company = $(boxes[i]).find('.company>span>a').text().trim();
            if (!job.company) job.company = $(boxes[i]).find('.company').text().trim();

            job.location = $(boxes[i]).find('.location>span').text().trim();
            if (!job.location) job.location = $(boxes[i]).find('span.location').text().trim();

            var href = $(boxes[i]).find('a.turnstileLink').attr('href');
            job.url = 'https://www.indeed.com' + href;

            // Update the last job posting for this zip
            if (i === 0) lastJob = job;

            // Break if we reached the point we last polled
            if (job.name === curJob.name) break;
            else publishJob(job, this.zips[index].geo);
        }

        this.zips[index].job = lastJob;

    }).then(() => {
        if (index + 1 < this.zips.length) check(index + 1);
        else {
            console.log("Finished updating one cycle.");
            check(0);
        }
    });
};

// Send a message in satori
var publishJob = (job, geo) => {
    job.geo = geo;
    rtm.publish(config.channel, job);
};

