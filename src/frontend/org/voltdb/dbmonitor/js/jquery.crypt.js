/*
 * jQuery Cryptography Plug-in
 * version: 1.0.0 (24 Sep 2008)
 * copyright 2008 Scott Thompson http://www.itsyndicate.ca - scott@itsyndicate.ca
 * http://www.opensource.org/licenses/mit-license.php
 *
 * A set of functions to do some basic cryptography encoding/decoding
 * I compiled from some javascripts I found into a jQuery plug-in.
 * Thanks go out to the original authors.
 *
 * Also a big thanks to Wade W. Hedgren http://homepages.uc.edu/~hedgreww
 * for the 1.1.1 upgrade to conform correctly to RFC4648 Sec5 url save base64
 *
 * Changelog: 1.1.0
 * - rewrote plugin to use only one item in the namespace
 *
 * Changelog: 1.1.1
 * - added code to base64 to allow URL and Filename Safe Alphabet (RFC4648 Sec5) 
 *
 * --- Base64 Encoding and Decoding code was written by
 *
 * Base64 code from Tyler Akins -- http://rumkin.com
 * and is placed in the public domain
 *
 *
 * --- MD5 and SHA1 Functions based upon Paul Johnston's javascript libraries.
 * A JavaScript implementation of the RSA Data Security, Inc. MD5 Message
 * Digest Algorithm, as defined in RFC 1321.
 * Version 2.1 Copyright (C) Paul Johnston 1999 - 2002.
 * Other contributors: Greg Holt, Andrew Kepert, Ydnar, Lostinet
 * Distributed under the BSD License
 * See http://pajhome.org.uk/crypt/md5 for more info.
 *
 * xTea Encrypt and Decrypt
 * copyright 2000-2005 Chris Veness
 * http://www.movable-type.co.uk
 *
 *
 * Examples:
 *
        var md5 = $().crypt({method:"md5",source:$("#phrase").val()});
        var sha1 = $().crypt({method:"sha1",source:$("#phrase").val()});
        var b64 = $().crypt({method:"b64enc",source:$("#phrase").val()});
        var b64dec = $().crypt({method:"b64dec",source:b64});
        var xtea = $().crypt({method:"xteaenc",source:$("#phrase").val(),keyPass:$("#passPhrase").val()});
        var xteadec = $().crypt({method:"xteadec",source:xtea,keyPass:$("#passPhrase").val()});
        var xteab64 = $().crypt({method:"xteab64enc",source:$("#phrase").val(),keyPass:$("#passPhrase").val()});
        var xteab64dec = $().crypt({method:"xteab64dec",source:xteab64,keyPass:$("#passPhrase").val()});

    You can also pass source this way.
    var md5 = $("#idOfSource").crypt({method:"md5"});
 *
 */
(function($){
     $.fn.crypt = function(options) {
        var defaults = {
            b64Str  : "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
            strKey  : "123",
            method  : "md5",
            source  : "",
            chrsz   : 8, /* md5 - bits per input character. 8 - ASCII; 16 - Unicode      */
            hexcase : 0  /* md5 - hex output format. 0 - lowercase; 1 - uppercase        */
        };
        
        // code to enable URL and Filename Safe Alphabet (RFC4648 Sec5)
        if (typeof(options.urlsafe) == 'undefined'){
            defaults.b64Str += '+/=';
            options.urlsafe = false;
        }else if (options.urlsafe){
            defaults.b64Str += '-_=';
        }else{
            defaults.b64Str += '+/=';
        }

        var opts = $.extend(defaults, options);
        
        // support for $("#name").crypt.....
        if (!opts.source) {
            var $this = $(this);
            // determine if it's a div or a textarea
            if ($this.html()) opts.source = $this.html();
            else if ($this.val()) opts.source = $this.val();
            else {alert("Please provide source text");return false;};
        };

        if (opts.method == 'md5') {
            return md5(opts);
        } else if (opts.method == 'sha1') {
            return sha1(opts);
        } else if (opts.method == 'b64enc') {
            return b64enc(opts);
        } else if (opts.method == 'b64dec') {
            return b64dec(opts);
        } else if (opts.method == 'xteaenc') {
            return xteaenc(opts);
        } else if (opts.method == 'xteadec') {
            return xteadec(opts);
        } else if (opts.method == 'xteab64enc') {
            var tmpenc = xteaenc(opts);
            opts.method = "b64enc";
            opts.source = tmpenc;
            return b64enc(opts);
        } else if (opts.method == 'xteab64dec') {
            var tmpdec = b64dec(opts);
            opts.method = "xteadec";
            opts.source = tmpdec;
            return xteadec(opts);
        }


        function b64enc(params) {

            var output = "";
            var chr1, chr2, chr3;
            var enc1, enc2, enc3, enc4;
            var i = 0;

            do {
                chr1 = params.source.charCodeAt(i++);
                chr2 = params.source.charCodeAt(i++);
                chr3 = params.source.charCodeAt(i++);

                enc1 = chr1 >> 2;
                enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);
                enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);
                enc4 = chr3 & 63;

                if (isNaN(chr2)) {
                    enc3 = enc4 = 64;
                } else if (isNaN(chr3)) {
                    enc4 = 64;
                };

                output += params.b64Str.charAt(enc1)
                    + params.b64Str.charAt(enc2)
                    + params.b64Str.charAt(enc3)
                    + params.b64Str.charAt(enc4);


            } while (i < params.source.length);

            return output;

        };

        function b64dec(params) {
                    
            var output = "";
            var chr1, chr2, chr3;
            var enc1, enc2, enc3, enc4;
            var i = 0;

            // remove all characters that are not A-Z, a-z, 0-9, !, -, or _
            
            // remove all characters that are not A-Z, a-z, 0-9, !, -, or _
            // params.source = params.source.replace(/[^A-Za-z0-9!_-]/g, "");
            
            var re = new RegExp ('[^A-Za-z0-9' + params.b64Str.substr(-3) + ']', 'g');
            params.source = params.source.replace(re, "");

            do {
                enc1 = params.b64Str.indexOf(params.source.charAt(i++));
                enc2 = params.b64Str.indexOf(params.source.charAt(i++));
                enc3 = params.b64Str.indexOf(params.source.charAt(i++));
                enc4 = params.b64Str.indexOf(params.source.charAt(i++));

                chr1 = (enc1 << 2) | (enc2 >> 4);
                chr2 = ((enc2 & 15) << 4) | (enc3 >> 2);
                chr3 = ((enc3 & 3) << 6) | enc4;

                output = output + String.fromCharCode(chr1);

                if (enc3 != 64) {
                    output = output + String.fromCharCode(chr2);
                }
                if (enc4 != 64) {
                    output = output + String.fromCharCode(chr3);
                }
            } while (i < params.source.length);

            return output;
        };


        function md5(params) {
            /* This is a trimmed version of Paul Johnsons JavaScript
             *
             * A JavaScript implementation of the RSA Data Security, Inc. MD5 Message
             * Digest Algorithm, as defined in RFC 1321.
             * Version 2.1 Copyright (C) Paul Johnston 1999 - 2002.
             * Other contributors: Greg Holt, Andrew Kepert, Ydnar, Lostinet
             * Distributed under the BSD License
             * See http://pajhome.org.uk/crypt/md5 for more info.
             */

            //var chrsz   = 8;  /* bits per input character. 8 - ASCII; 16 - Unicode      */
            //var hexcase = 0;  /* hex output format. 0 - lowercase; 1 - uppercase        */

            return binl2hex(core_md5(str2binl(params.source), params.source.length * params.chrsz));

            /*
             * Convert an array of little-endian words to a hex string.
             */
            function binl2hex(binarray)
            {
              var hex_tab = params.hexcase ? "0123456789ABCDEF" : "0123456789abcdef";
              var str = "";
              for(var i = 0; i < binarray.length * 4; i++)
              {
                str += hex_tab.charAt((binarray[i>>2] >> ((i%4)*8+4)) & 0xF) +
                       hex_tab.charAt((binarray[i>>2] >> ((i%4)*8  )) & 0xF);
              };
              return str;
            };

            /*
             * Calculate the HMAC-MD5, of a key and some data
             */
            function core_hmac_md5(key, data)
            {
              var bkey = str2binl(key);
              if(bkey.length > 16) bkey = core_md5(bkey, key.length * params.chrsz);

              var ipad = Array(16), opad = Array(16);
              for(var i = 0; i < 16; i++)
              {
                ipad[i] = bkey[i] ^ 0x36363636;
                opad[i] = bkey[i] ^ 0x5C5C5C5C;
              };

              var hash = core_md5(ipad.concat(str2binl(data)), 512 + data.length * params.chrsz);
              return core_md5(opad.concat(hash), 512 + 128);
            };

            /*
             * Convert a string to an array of little-endian words
             * If chrsz is ASCII, characters >255 have their hi-byte silently ignored.
             */
            function str2binl(str)
            {
              var bin = Array();
              var mask = (1 << params.chrsz) - 1;
              for(var i = 0; i < str.length * params.chrsz; i += params.chrsz)
                bin[i>>5] |= (str.charCodeAt(i / params.chrsz) & mask) << (i%32);
              return bin;
            }


            /*
             * Bitwise rotate a 32-bit number to the left.
             */
            function bit_rol(num, cnt)
            {
              return (num << cnt) | (num >>> (32 - cnt));
            }


            /*
             * These functions implement the four basic operations the algorithm uses.
             */
            function md5_cmn(q, a, b, x, s, t)
            {
              return safe_add(bit_rol(safe_add(safe_add(a, q), safe_add(x, t)), s),b);
            }
            function md5_ff(a, b, c, d, x, s, t)
            {
              return md5_cmn((b & c) | ((~b) & d), a, b, x, s, t);
            }
            function md5_gg(a, b, c, d, x, s, t)
            {
              return md5_cmn((b & d) | (c & (~d)), a, b, x, s, t);
            }
            function md5_hh(a, b, c, d, x, s, t)
            {
              return md5_cmn(b ^ c ^ d, a, b, x, s, t);
            }
            function md5_ii(a, b, c, d, x, s, t)
            {
              return md5_cmn(c ^ (b | (~d)), a, b, x, s, t);
            }

            /*
             * Calculate the MD5 of an array of little-endian words, and a bit length
             */
            function core_md5(x, len)
            {
              /* append padding */
              x[len >> 5] |= 0x80 << ((len) % 32);
              x[(((len + 64) >>> 9) << 4) + 14] = len;

              var a =  1732584193;
              var b = -271733879;
              var c = -1732584194;
              var d =  271733878;

              for(var i = 0; i < x.length; i += 16)
              {
                var olda = a;
                var oldb = b;
                var oldc = c;
                var oldd = d;

                a = md5_ff(a, b, c, d, x[i+ 0], 7 , -680876936);
                d = md5_ff(d, a, b, c, x[i+ 1], 12, -389564586);
                c = md5_ff(c, d, a, b, x[i+ 2], 17,  606105819);
                b = md5_ff(b, c, d, a, x[i+ 3], 22, -1044525330);
                a = md5_ff(a, b, c, d, x[i+ 4], 7 , -176418897);
                d = md5_ff(d, a, b, c, x[i+ 5], 12,  1200080426);
                c = md5_ff(c, d, a, b, x[i+ 6], 17, -1473231341);
                b = md5_ff(b, c, d, a, x[i+ 7], 22, -45705983);
                a = md5_ff(a, b, c, d, x[i+ 8], 7 ,  1770035416);
                d = md5_ff(d, a, b, c, x[i+ 9], 12, -1958414417);
                c = md5_ff(c, d, a, b, x[i+10], 17, -42063);
                b = md5_ff(b, c, d, a, x[i+11], 22, -1990404162);
                a = md5_ff(a, b, c, d, x[i+12], 7 ,  1804603682);
                d = md5_ff(d, a, b, c, x[i+13], 12, -40341101);
                c = md5_ff(c, d, a, b, x[i+14], 17, -1502002290);
                b = md5_ff(b, c, d, a, x[i+15], 22,  1236535329);

                a = md5_gg(a, b, c, d, x[i+ 1], 5 , -165796510);
                d = md5_gg(d, a, b, c, x[i+ 6], 9 , -1069501632);
                c = md5_gg(c, d, a, b, x[i+11], 14,  643717713);
                b = md5_gg(b, c, d, a, x[i+ 0], 20, -373897302);
                a = md5_gg(a, b, c, d, x[i+ 5], 5 , -701558691);
                d = md5_gg(d, a, b, c, x[i+10], 9 ,  38016083);
                c = md5_gg(c, d, a, b, x[i+15], 14, -660478335);
                b = md5_gg(b, c, d, a, x[i+ 4], 20, -405537848);
                a = md5_gg(a, b, c, d, x[i+ 9], 5 ,  568446438);
                d = md5_gg(d, a, b, c, x[i+14], 9 , -1019803690);
                c = md5_gg(c, d, a, b, x[i+ 3], 14, -187363961);
                b = md5_gg(b, c, d, a, x[i+ 8], 20,  1163531501);
                a = md5_gg(a, b, c, d, x[i+13], 5 , -1444681467);
                d = md5_gg(d, a, b, c, x[i+ 2], 9 , -51403784);
                c = md5_gg(c, d, a, b, x[i+ 7], 14,  1735328473);
                b = md5_gg(b, c, d, a, x[i+12], 20, -1926607734);

                a = md5_hh(a, b, c, d, x[i+ 5], 4 , -378558);
                d = md5_hh(d, a, b, c, x[i+ 8], 11, -2022574463);
                c = md5_hh(c, d, a, b, x[i+11], 16,  1839030562);
                b = md5_hh(b, c, d, a, x[i+14], 23, -35309556);
                a = md5_hh(a, b, c, d, x[i+ 1], 4 , -1530992060);
                d = md5_hh(d, a, b, c, x[i+ 4], 11,  1272893353);
                c = md5_hh(c, d, a, b, x[i+ 7], 16, -155497632);
                b = md5_hh(b, c, d, a, x[i+10], 23, -1094730640);
                a = md5_hh(a, b, c, d, x[i+13], 4 ,  681279174);
                d = md5_hh(d, a, b, c, x[i+ 0], 11, -358537222);
                c = md5_hh(c, d, a, b, x[i+ 3], 16, -722521979);
                b = md5_hh(b, c, d, a, x[i+ 6], 23,  76029189);
                a = md5_hh(a, b, c, d, x[i+ 9], 4 , -640364487);
                d = md5_hh(d, a, b, c, x[i+12], 11, -421815835);
                c = md5_hh(c, d, a, b, x[i+15], 16,  530742520);
                b = md5_hh(b, c, d, a, x[i+ 2], 23, -995338651);

                a = md5_ii(a, b, c, d, x[i+ 0], 6 , -198630844);
                d = md5_ii(d, a, b, c, x[i+ 7], 10,  1126891415);
                c = md5_ii(c, d, a, b, x[i+14], 15, -1416354905);
                b = md5_ii(b, c, d, a, x[i+ 5], 21, -57434055);
                a = md5_ii(a, b, c, d, x[i+12], 6 ,  1700485571);
                d = md5_ii(d, a, b, c, x[i+ 3], 10, -1894986606);
                c = md5_ii(c, d, a, b, x[i+10], 15, -1051523);
                b = md5_ii(b, c, d, a, x[i+ 1], 21, -2054922799);
                a = md5_ii(a, b, c, d, x[i+ 8], 6 ,  1873313359);
                d = md5_ii(d, a, b, c, x[i+15], 10, -30611744);
                c = md5_ii(c, d, a, b, x[i+ 6], 15, -1560198380);
                b = md5_ii(b, c, d, a, x[i+13], 21,  1309151649);
                a = md5_ii(a, b, c, d, x[i+ 4], 6 , -145523070);
                d = md5_ii(d, a, b, c, x[i+11], 10, -1120210379);
                c = md5_ii(c, d, a, b, x[i+ 2], 15,  718787259);
                b = md5_ii(b, c, d, a, x[i+ 9], 21, -343485551);

                a = safe_add(a, olda);
                b = safe_add(b, oldb);
                c = safe_add(c, oldc);
                d = safe_add(d, oldd);
              };
              return Array(a, b, c, d);

            };

        };

        /*
         * Add integers, wrapping at 2^32. This uses 16-bit operations internally
         * to work around bugs in some JS interpreters. (used by md5 and sha1)
         */
        function safe_add(x, y)
        {
          var lsw = (x & 0xFFFF) + (y & 0xFFFF);
          var msw = (x >> 16) + (y >> 16) + (lsw >> 16);
          return (msw << 16) | (lsw & 0xFFFF);
        };

        function sha1(params) {
            return binb2hex(core_sha1(str2binb(params.source),params.source.length * params.chrsz));

            /*
             * Calculate the SHA-1 of an array of big-endian words, and a bit length
             */
            function core_sha1(x, len)
            {
              /* append padding */
              x[len >> 5] |= 0x80 << (24 - len % 32);
              x[((len + 64 >> 9) << 4) + 15] = len;

              var w = Array(80);
              var a =  1732584193;
              var b = -271733879;
              var c = -1732584194;
              var d =  271733878;
              var e = -1009589776;

              for(var i = 0; i < x.length; i += 16)
              {
                var olda = a;
                var oldb = b;
                var oldc = c;
                var oldd = d;
                var olde = e;

                for(var j = 0; j < 80; j++)
                {
                  if(j < 16) w[j] = x[i + j];
                  else w[j] = rol(w[j-3] ^ w[j-8] ^ w[j-14] ^ w[j-16], 1);
                  var t = safe_add(safe_add(rol(a, 5), sha1_ft(j, b, c, d)),
                                   safe_add(safe_add(e, w[j]), sha1_kt(j)));
                  e = d;
                  d = c;
                  c = rol(b, 30);
                  b = a;
                  a = t;
                }

                a = safe_add(a, olda);
                b = safe_add(b, oldb);
                c = safe_add(c, oldc);
                d = safe_add(d, oldd);
                e = safe_add(e, olde);
              }
              return Array(a, b, c, d, e);

            }
            /*
             * Bitwise rotate a 32-bit number to the left.
             */
            function rol(num, cnt)
            {
              return (num << cnt) | (num >>> (32 - cnt));
            }

            /*
             * Determine the appropriate additive constant for the current iteration
             */
            function sha1_kt(t)
            {
              return (t < 20) ?  1518500249 : (t < 40) ?  1859775393 :
                     (t < 60) ? -1894007588 : -899497514;
            }
            /*
             * Perform the appropriate triplet combination function for the current
             * iteration
             */
            function sha1_ft(t, b, c, d)
            {
              if(t < 20) return (b & c) | ((~b) & d);
              if(t < 40) return b ^ c ^ d;
              if(t < 60) return (b & c) | (b & d) | (c & d);
              return b ^ c ^ d;
            }

            /*
             * Convert an array of big-endian words to a hex string.
             */
            function binb2hex(binarray)
            {
              var hex_tab = params.hexcase ? "0123456789ABCDEF" : "0123456789abcdef";
              var str = "";
              for(var i = 0; i < binarray.length * 4; i++)
              {
                str += hex_tab.charAt((binarray[i>>2] >> ((3 - i%4)*8+4)) & 0xF) +
                       hex_tab.charAt((binarray[i>>2] >> ((3 - i%4)*8  )) & 0xF);
              }
              return str;
            }


            /*
             * Convert an 8-bit or 16-bit string to an array of big-endian words
             * In 8-bit function, characters >255 have their hi-byte silently ignored.
             */
            function str2binb(str)
            {
              var bin = Array();
              var mask = (1 << params.chrsz) - 1;
              for(var i = 0; i < str.length * params.chrsz; i += params.chrsz)
                bin[i>>5] |= (str.charCodeAt(i / params.chrsz) & mask) << (32 - params.chrsz - i%32);
              return bin;
            }

        };

        function xteaenc(params) {
            var v = new Array(2), k = new Array(4), s = "", i;

            params.source = escape(params.source);  // use escape() so only have single-byte chars to encode

            // build key directly from 1st 16 chars of strKey
            for (var i=0; i<4; i++) k[i] = Str4ToLong(params.strKey.slice(i*4,(i+1)*4));

            for (i=0; i<params.source.length; i+=8) {  // encode strSource into s in 64-bit (8 char) blocks
                v[0] = Str4ToLong(params.source.slice(i,i+4));  // ... note this is 'electronic codebook' mode
                v[1] = Str4ToLong(params.source.slice(i+4,i+8));
                code(v, k);
                s += LongToStr4(v[0]) + LongToStr4(v[1]);
            }

            return escCtrlCh(s);
            // note: if strSource or strKey are passed as string objects, rather than strings, this
            // function will throw an 'Object doesn't support this property or method' error

            function code(v, k) {
              // Extended TEA: this is the 1997 revised version of Needham & Wheeler's algorithm
              // params: v[2] 64-bit value block; k[4] 128-bit key
              var y = v[0], z = v[1];
              var delta = 0x9E3779B9, limit = delta*32, sum = 0;

              while (sum != limit) {
                y += (z<<4 ^ z>>>5)+z ^ sum+k[sum & 3];
                sum += delta;
                z += (y<<4 ^ y>>>5)+y ^ sum+k[sum>>>11 & 3];
                // note: unsigned right-shift '>>>' is used in place of original '>>', due to lack
                // of 'unsigned' type declaration in JavaScript (thanks to Karsten Kraus for this)
              }
              v[0] = y;v[1] = z;
            }
        };

        function xteadec(params) {
            var v = new Array(2), k = new Array(4), s = "", i;

            for (var i=0; i<4; i++) k[i] = Str4ToLong(params.strKey.slice(i*4,(i+1)*4));

            ciphertext = unescCtrlCh(params.source);
            for (i=0; i<ciphertext.length; i+=8) {  // decode ciphertext into s in 64-bit (8 char) blocks
                v[0] = Str4ToLong(ciphertext.slice(i,i+4));
                v[1] = Str4ToLong(ciphertext.slice(i+4,i+8));
                decode(v, k);
                s += LongToStr4(v[0]) + LongToStr4(v[1]);
            }

            // strip trailing null chars resulting from filling 4-char blocks:
            s = s.replace(/\0+$/, '');

            return unescape(s);


            function decode(v, k) {
              var y = v[0], z = v[1];
              var delta = 0x9E3779B9, sum = delta*32;

              while (sum != 0) {
                z -= (y<<4 ^ y>>>5)+y ^ sum+k[sum>>>11 & 3];
                sum -= delta;
                y -= (z<<4 ^ z>>>5)+z ^ sum+k[sum & 3];
              }
              v[0] = y;v[1] = z;
            }

        };

            // xtea supporting functions
        function Str4ToLong(s) {  // convert 4 chars of s to a numeric long
          var v = 0;
          for (var i=0; i<4; i++) v |= s.charCodeAt(i) << i*8;
          return isNaN(v) ? 0 : v;
        };

        function LongToStr4(v) {  // convert a numeric long to 4 char string
          var s = String.fromCharCode(v & 0xFF, v>>8 & 0xFF, v>>16 & 0xFF, v>>24 & 0xFF);
          return s;
        };

        function escCtrlCh(str) {  // escape control chars which might cause problems with encrypted texts
          return str.replace(/[\0\t\n\v\f\r\xa0'"!]/g, function(c) {return '!' + c.charCodeAt(0) + '!';});
        };

        function unescCtrlCh(str) {  // unescape potentially problematic nulls and control characters
          return str.replace(/!\d\d?\d?!/g, function(c) {return String.fromCharCode(c.slice(1,-1));});
        };



    };
})(jQuery);


