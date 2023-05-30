const puppeteer = require('puppeteer');
const {writeFile} = require('fs').promises;
const cliProgress = require('cli-progress');

async function computeCsvStringFromTable(page, tableSelector, shouldIncludeRowHeaders) {
    const csvString = await page.evaluate((tableSelector, shouldIncludeRowHeaders) => {
        const table = document.querySelector(tableSelector);
        if (!table) {
            return null;
        }
        
        let csvString = "";
        for (let i = 0; i < table.rows.length; i++) {
            const row = table.rows[i];

            if (!shouldIncludeRowHeaders && i === 0) {
                continue;
            }

            for (let j = 0; j < row.cells.length; j++) {
                const cell = row.cells[j];

                const formattedCellText = cell.innerText.replace(/\n/g, '\\n').trim();
                if (formattedCellText !== "No Data") {
                    csvString += formattedCellText;
                }
                
                if (j === row.cells.length - 1) {
                    csvString += "\n";
                } else {
                    csvString += ",";
                }
            }
        }
        return csvString;
    }, tableSelector, shouldIncludeRowHeaders);

    return csvString;
}

(async () => {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    await page.setViewport({ width: 1920, height: 1080 });
    await page.goto('https://www.mta.maryland.gov/performance-improvement');

    await page.click('h3#ui-id-5');

    let csvString = "";

    const routeSelectSelector = 'select[name="ridership-select-route"]';
    const routeSelectOptions = await page.$$eval(`${routeSelectSelector} > option`, options => { return options.map(option => option.value) });
    console.log('routes available:', routeSelectOptions);

    const monthSelectSelector = 'select[name="ridership-select-month"]';
    const monthSelectOptions = await page.$$eval(`${monthSelectSelector} > option`, options => { return options.map(option => option.value) });

    console.log('months available:', monthSelectOptions);

    const yearSelectSelector = 'select[name="ridership-select-year"]';
    const yearSelectOptions = await page.$$eval(`${yearSelectSelector} > option`, options => { return options.map(option => option.value) });
    console.log('years available:', yearSelectOptions);

    const progressBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
    progressBar.start(monthSelectOptions.length * yearSelectOptions.length, 0);
    
    let hasIncludedRowHeaders = true;
    for (const yearSelectOption of yearSelectOptions) {
        await page.focus(yearSelectSelector);
        await page.select(yearSelectSelector, yearSelectOption);
        
        for (const monthSelectOption of monthSelectOptions) {
            await page.focus(monthSelectSelector);
            await page.select(monthSelectSelector, monthSelectOption);

            await page.keyboard.press("Tab");
            await page.keyboard.press("Tab");

            await Promise.all([
                page.keyboard.press('Enter'),
                page.waitForNetworkIdle()
            ]);
            
            csvString += await computeCsvStringFromTable(page, 'div#container-ridership-table > table', hasIncludedRowHeaders);

            if (hasIncludedRowHeaders) {
                hasIncludedRowHeaders = false;
            }

            progressBar.increment();
        }
    }

    progressBar.stop();

    await browser.close();

    // await writeFile("../data/raw/mta_bus_ridership.csv", csvString);
    await writeFile("data/raw/mta_bus_ridership.csv", csvString);
})();