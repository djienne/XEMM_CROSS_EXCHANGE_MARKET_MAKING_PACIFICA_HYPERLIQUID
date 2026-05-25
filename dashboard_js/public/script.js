const API_BASE = 'http://localhost:3000/api';

// DOM Elements
const statusBadge = document.getElementById('status-indicator');
const statusText = document.getElementById('status-text');
const actionOutput = document.getElementById('action-output');
const logViewer = document.getElementById('log-viewer');
const tradesBody = document.getElementById('trades-body');

// Initial Load
document.addEventListener('DOMContentLoaded', () => {
    scheduleStatusCheck();
    fetchConfig();
    fetchLogs();
    fetchTrades();
    scheduleLogRefresh();
});

let isCheckingStatus = false;

async function scheduleStatusCheck() {
    if (isCheckingStatus) return;
    await checkStatus();
    setTimeout(scheduleStatusCheck, 10000);
}

async function checkStatus() {
    if (isCheckingStatus) return;
    isCheckingStatus = true;

    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 15000);

        const response = await fetch(`${API_BASE}/status`, { signal: controller.signal });
        clearTimeout(timeoutId);

        const data = await response.json();

        statusBadge.className = 'status-badge';
        const statusDetail = document.getElementById('status-detail');

        if (data.status === 'RUNNING') {
            statusBadge.classList.add('running');
            statusText.textContent = 'Running';
            statusDetail.textContent = 'OK Bot is running for 1 cycle (will stop automatically when cycle completes). Click "Stop Bot" to interrupt.';
            statusDetail.className = 'status-description running';
        } else if (data.status === 'STOPPED') {
            statusBadge.classList.add('stopped');
            statusText.textContent = 'Stopped';
            statusDetail.textContent = 'pause Bot is stopped and waiting for user to start. Click "Start Bot" to begin trading.';
            statusDetail.className = 'status-description stopped';
        } else {
            statusBadge.classList.add('unknown');
            statusText.textContent = 'Unknown';
            statusDetail.textContent = 'WARN Unable to determine bot status. Check remote connection.';
            statusDetail.className = 'status-description unknown';
        }
    } catch (error) {
        console.error('Error checking status:', error);
        statusText.textContent = error.name === 'AbortError' ? 'Timeout' : 'Error';
        statusBadge.className = 'status-badge unknown';
        document.getElementById('status-detail').textContent = 'ERROR Connection error. Unable to reach remote server.';
        document.getElementById('status-detail').className = 'status-description error';
    } finally {
        isCheckingStatus = false;
    }
}

async function fetchConfig() {
    const configViewer = document.getElementById('config-viewer');
    try {
        const response = await fetch(`${API_BASE}/config`);
        const data = await response.json();

        if (data.error) {
            configViewer.textContent = `Error: ${data.error}`;
            return;
        }

        configViewer.innerHTML = '';
        const excludedKeys = [
            'reconnect_attempts',
            'ping_interval_secs',
            'hyperliquid_slippage',
            'pacifica_rest_poll_interval_secs',
            'pacifica_active_order_rest_poll_interval_ms'
        ];

        for (const [key, value] of Object.entries(data)) {
            if (excludedKeys.includes(key)) continue;

            const item = document.createElement('div');
            item.className = 'config-item';
            item.innerHTML = `
                <span class="config-key">${key}</span>
                <span class="config-value">${value}</span>
            `;
            configViewer.appendChild(item);
        }
    } catch (error) {
        configViewer.textContent = `Error loading config: ${error.message}`;
    }
}

function startBot() {
    if (!confirm('Are you sure you want to START the bot?')) {
        return;
    }

    actionOutput.classList.remove('hidden');
    actionOutput.textContent = 'Starting bot...';

    fetch(`${API_BASE}/start`, { method: 'POST' })
        .then(response => response.json())
        .then(data => {
            let output = `[Start Bot Result]\n`;
            output += `Success: ${data.success}\n`;
            if (data.stdout) output += `\nSTDOUT:\n${data.stdout}\n`;
            if (data.stderr) output += `\nSTDERR:\n${data.stderr}\n`;

            actionOutput.textContent = output;

            // Refresh logs and status after starting
            setTimeout(() => {
                fetchLogs();
                checkStatus();
            }, 2000);
        })
        .catch(error => {
            actionOutput.textContent = `Error: ${error.message}`;
        });
}

async function stopBot() {
    if (!confirm('Are you sure you want to STOP the bot?')) {
        return;
    }

    actionOutput.classList.remove('hidden');
    actionOutput.textContent = 'Stopping bot...';

    try {
        const response = await fetch(`${API_BASE}/stop`, { method: 'POST' });
        const data = await response.json();

        let output = `[Stop Bot Result]\n`;
        output += `Success: ${data.success}\n`;
        if (data.stdout) output += `\nSTDOUT:\n${data.stdout}\n`;
        if (data.stderr) output += `\nSTDERR:\n${data.stderr}\n`;

        actionOutput.textContent = output;

        // Refresh logs and status after stopping
        setTimeout(() => {
            fetchLogs();
            checkStatus();
        }, 1000);
    } catch (error) {
        actionOutput.textContent = `Error: ${error.message}`;
    }
}

function deployBot() {
    if (confirm('Are you sure you want to DEPLOY? This will overwrite remote files.')) {
        runAction('deploy', 'Deploy');
    }
}

async function runAction(endpoint, name) {
    actionOutput.classList.remove('hidden');
    actionOutput.textContent = `Executing ${name}...`;

    try {
        const response = await fetch(`${API_BASE}/${endpoint}`, { method: 'POST' });
        const data = await response.json();

        let output = `[${name} Result]\n`;
        output += `Success: ${data.success}\n`;
        if (data.stdout) output += `\nSTDOUT:\n${data.stdout}\n`;
        if (data.stderr) output += `\nSTDERR:\n${data.stderr}\n`;

        actionOutput.textContent = output;

        setTimeout(checkStatus, 2000);
    } catch (error) {
        actionOutput.textContent = `Error executing ${name}: ${error.message}`;
    }
}

async function fetchLogs() {
    const lines = document.getElementById('log-lines').value;

    try {
        const response = await fetch(`${API_BASE}/logs?lines=${lines}`);
        const data = await response.json();

        if (data.success) {
            logViewer.style.opacity = '0.5';

            setTimeout(() => {
                logViewer.innerHTML = ansiToHtml(data.stdout || 'No logs returned.');
                logViewer.scrollTop = logViewer.scrollHeight;
                logViewer.style.opacity = '1';
            }, 200);
        } else {
            logViewer.textContent = `Error fetching logs:\n${data.stderr}`;
        }
    } catch (error) {
        logViewer.textContent = `Network error: ${error.message}`;
    }
}

function scheduleLogRefresh() {
    fetchLogs();
    setTimeout(scheduleLogRefresh, 15000);
}

function ansiToHtml(text) {
    if (!text) return '';

    const codes = {
        '0': 'reset',
        '1': 'font-weight: bold',
        '2': 'opacity: 0.7',
        '30': 'color: #000',
        '31': 'color: #ef4444',
        '32': 'color: #22c55e',
        '33': 'color: #eab308',
        '34': 'color: #3b82f6',
        '35': 'color: #a855f7',
        '36': 'color: #06b6d4',
        '37': 'color: #fff',
    };

    let html = text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\u001b\[(\d+)(?:;(\d+))?m/g, (match, code1, code2) => {
            if (code1 === '0') return '</span></span>';

            let style = codes[code1] || '';
            if (code2 && codes[code2]) style += '; ' + codes[code2];

            if (style === 'reset') return '</span></span>';

            return `<span style="${style}">`;
        });

    return html.replace(/\n/g, '<br>');
}

async function syncTrades() {
    if (confirm('Sync trades from remote? This may take a moment.')) {
        await runAction('sync_trades', 'Sync Trades');
        fetchTrades();
    }
}

async function refreshTradeStats() {
    const btn = event.target || event.srcElement;
    const originalText = btn.textContent;
    btn.textContent = 'Downloading...';
    btn.disabled = true;

    try {
        const response = await fetch(`${API_BASE}/sync_trades`, { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            await fetchTrades();
        } else {
            console.error('Failed to download trades:', data.stderr);
            alert('Failed to download trades. Check console for details.');
        }
    } catch (error) {
        console.error('Error downloading trades:', error);
        alert('Error downloading trades: ' + error.message);
    } finally {
        btn.textContent = originalText;
        btn.disabled = false;
    }
}

async function fetchTrades() {
    tradesBody.innerHTML = '<tr><td colspan="5" class="text-center">Loading trades...</td></tr>';

    try {
        const response = await fetch(`${API_BASE}/trades`);
        const data = await response.json();

        if (data.trades && data.trades.length > 0) {
            tradesBody.innerHTML = '';
            data.trades.forEach(trade => {
                const row = document.createElement('tr');

                const profit = parseFloat(trade.actual_profit_usd);
                const profitClass = profit >= 0 ? 'profit-pos' : 'profit-neg';

                row.innerHTML = `
                    <td>${trade.timestamp}</td>
                    <td>${trade.symbol}</td>
                    <td>${trade.pacifica_side}/${trade.hyperliquid_side}</td>
                    <td class="${profitClass}">$${profit.toFixed(4)}</td>
                    <td>${parseFloat(trade.latency_ms).toFixed(1)}ms</td>
                `;
                tradesBody.appendChild(row);
            });

            const tableContainer = document.querySelector('.trades .table-container');
            if (tableContainer) {
                tableContainer.scrollLeft = tableContainer.scrollWidth;
            }

            updateTradeStats(data.trades);
        } else {
            tradesBody.innerHTML = '<tr><td colspan="5" class="text-center">No trades found. Try syncing.</td></tr>';
            updateTradeStats([]);
        }
    } catch (error) {
        tradesBody.innerHTML = `<tr><td colspan="5" class="text-center">Error: ${error.message}</td></tr>`;
    }
}

function updateTradeStats(trades) {
    if (!trades || trades.length === 0) {
        document.getElementById('stat-total').textContent = '0';
        document.getElementById('stat-pnl').textContent = '$0.00';
        document.getElementById('stat-avg').textContent = '$0.00';
        document.getElementById('stat-winrate').textContent = '0%';
        document.getElementById('stat-symbol').textContent = '-';
        document.getElementById('stat-latency').textContent = '0ms';
        return;
    }

    const total = trades.length;
    const totalPnL = trades.reduce((sum, t) => sum + parseFloat(t.actual_profit_usd || 0), 0);
    const avgProfit = totalPnL / total;
    const wins = trades.filter(t => parseFloat(t.actual_profit_usd || 0) > 0).length;
    const winRate = (wins / total) * 100;
    const avgLatency = trades.reduce((sum, t) => sum + parseFloat(t.latency_ms || 0), 0) / total;
    const symbols = [...new Set(trades.map(t => t.symbol))].join(', ');

    document.getElementById('stat-total').textContent = total;
    document.getElementById('stat-pnl').textContent = `$${totalPnL.toFixed(2)}`;
    document.getElementById('stat-pnl').className = totalPnL >= 0 ? 'stat-value profit-pos' : 'stat-value profit-neg';
    document.getElementById('stat-avg').textContent = `$${avgProfit.toFixed(4)}`;
    document.getElementById('stat-avg').className = avgProfit >= 0 ? 'stat-value profit-pos' : 'stat-value profit-neg';
    document.getElementById('stat-winrate').textContent = `${winRate.toFixed(1)}%`;
    document.getElementById('stat-symbol').textContent = symbols;
    document.getElementById('stat-latency').textContent = `${avgLatency.toFixed(1)}ms`;
}
