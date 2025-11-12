#!/usr/bin/env node
import React, { createElement as h } from 'react';
import { render, Text, Box } from 'ink';
import dotenv from 'dotenv';

dotenv.config();

console.log('Starting test dashboard...');
console.log('HL_WALLET:', process.env.HL_WALLET ? 'SET' : 'NOT SET');
console.log('SOL_WALLET:', process.env.SOL_WALLET ? 'SET' : 'NOT SET');

const App = () => {
  return h(Box, { padding: 1 },
    h(Text, { color: 'green' }, 'Hello from Ink!')
  );
};

render(h(App));

console.log('Test dashboard rendered!');
