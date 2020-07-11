const { expect } = require('code');
const Lab = require('lab');
const lab = exports.lab = Lab.script();
const _ = require('lodash');

const utils = require('../utils');

lab.experiment('utils functions', () => {
    lab.test('test filterMaxInfo function', () => {
        const keyA = (item) => {
            return JSON.stringify(_.pick(item, ["a"]));
        };
        const keyB = (item) => {
            return JSON.stringify(_.pick(item, ["b"]));
        };
        const keyC = (item) => {
            return JSON.stringify(_.pick(item, ["c"]));
        };

        const o1 = { a: 1, b: 2, c: 3 };
        const o2 = { a: 1, b: 2, c: null };
        const o3 = { a: 1, b: null, c: null };
        const o4 = { a: null, b: null, c: null };
        const o5 = { a: null, b: null, c: 3 };
        const o6 = { a: null, b: 2, c: 3 };
        const o7 = { a: null, b: 2, c: null };
        const o8 = { a: 1, b: null, c: 3 };

        const d1 = [o1, o2, o3, o4, o5, o6, o7, o8];
        const d2 = [o2, o3, o4, o5, o6, o7, o8];

        expect(utils.filterMaxInfo(d1, keyA)).to.equal([o1, o6]);
        expect(utils.filterMaxInfo(d2, keyA)).to.equal([o2, o6]);

        expect(utils.filterMaxInfo(d1, keyB)).to.equal([o1, o8]);
        expect(utils.filterMaxInfo(d2, keyB)).to.equal([o2, o8]);

        expect(utils.filterMaxInfo(d1, keyC)).to.equal([o1, o2]);
        expect(utils.filterMaxInfo(d2, keyC)).to.equal([o2, o6]);

    });
    lab.test('test maxInfo function', () => {
        const o1 = { a: 1, b: 2, c: 3 };
        const o2 = { a: 1, b: 2, c: null };
        const o3 = { a: 1, b: null, c: null };
        const o4 = { a: null, b: null, c: null };
        const o5 = { a: null, b: null, c: 3 };
        const o6 = { a: null, b: 2, c: 3 };
        const o7 = { a: null, b: 2, c: null };
        const o8 = { a: 1, b: null, c: 3 };

        const a1 = [o1, o2, o3, o8];
        const a2 = [o2, o3, o8];
        const a3 = [o8, o3, o2];

        expect(utils.maxInfo(a1)).to.equal(o1);
        expect(utils.maxInfo(a2)).to.equal(o2);
        expect(utils.maxInfo(a3)).to.equal(o8);

        const b1 = [o1, o2, o6, o7];
        const b2 = [o2, o6, o7];
        const b3 = [o6, o2, o7];

        expect(utils.maxInfo(b1)).to.equal(o1);
        expect(utils.maxInfo(b2)).to.equal(o2);
        expect(utils.maxInfo(b3)).to.equal(o6);

        const c1 = [o1, o5, o6, o8];
        const c2 = [o5, o6, o8];
        const c3 = [o5, o8, o6];

        expect(utils.maxInfo(c1)).to.equal(o1);
        expect(utils.maxInfo(c2)).to.equal(o6);
        expect(utils.maxInfo(c3)).to.equal(o8);

        const x = [o4];

        expect(utils.maxInfo(x)).to.equal(o4);
    });
});