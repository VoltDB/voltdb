/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.utils;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

public class TestVoltTableUtil extends TestCase
{
    public void testCompressTable() throws Exception {
        String hex = "000001E780001B0906060606050404090909090904051906060605090319191919190000000249440000000E4C41535455504441544554494D450000000B4352454154454454494D450000000D54494D454F5554504552494F440000000B54494D454F55544C494E450000000553544154450000000B504152544954494F4E49440000000C53455353494F4E5354415445000000105345525645524944454E544946494552000000084E4F4445484F5354000000094E4F44455245414C4D0000000C4F524947494E414C484F53540000000D4F524947494E414C5245414C4D000000134C4153544343524551554553544E554D4245520000000E4C415354524553554C54434F44450000000C53455256494345494E464F53000000194D554C5449504C455345525649434553535550504F525445440000000C5355425343524942455249440000000844455649434549440000000E4944454E544946494552545950450000000A4944454E544946494552000000155553455245515549504D454E54494E464F54595045000000165553455245515549504D454E54494E464F56414C55450000001153455256494345504152414D45544552530000000E524154494E4753455353494F4E530000001041424D434C49454E5453455353494F4E00000008455854454E4445440000000100000D2500000061485943363030564E37594959543658373638564B57494932484E4E4D5550515133424A564B5142485549594F54463444514B584F5036365037375030354F43585A513656314B51474B48434937474C45354A383247424758555A4B46513445485800000138358331FB00000138358331FB00000000001032360000000000103236000003DD000003DD0000000A4D395651544643324F300000001E38544C584D5734384F464B36544F554E4D37325A44534F5057484F574E390000001E38544C584D5734384F464B36544F554E4D37325A44534F5057484F574E390000001E544D38324A443031414A44454139324F3554583830324C45545A444C4A4C0000001E544D38324A443031414A44454139324F3554583830324C45545A444C4A4C000C0000164000000521BDB26377F1525AE13C24FCCAEAA93CF289A73B65F0DAF3295D3814CB60AEC727C3A85A3595A31F977F8472117DAA826A9843EC844234469B5D3BFD7D427A6EF7B00F18D2E65FBDD155A23BF3207BCBBF92B468749368AD016E2F4D94AEEBF8F34761D3BF4D48EC409C03269B9512BE3F3D9ACCE9233304B54245A2A3CE608A0E6E81415783A65776F71E16C10F0359EDC1FC1A6ECA99A6B58C6618428296B769B05E623F1BA45DCC4E30A275B8F436192A7575A6712B51BD88D27D65DC4A886DBA223238F6F0139B4B1DA4602BF0F7BEE7E5DF60948E399C70CACE74A3AF1A5CDEF21FCBCCB9CA81DD6973F5FA7899394EC392F3C75F3A92084ECAC7D1AC520B93406C1EAAEE8758B93CB9032978C78F16EC1D9C2E602CB513E8B13815F33ECBF2A9D0EA6ED93492D4731038B1F66FF0DB11DE3103F4C6BEDB8A861C52DE1D0C3B6A135D4D047875EAC8E393FEA723F79DB186AD13F1A1CDAD7C546E1E282C0A4E5A687F4E707597FB053CA072440DF7419CC2BD41BD32BE5A54603A446AA7A03381C1A0CD1BEF70C8497023E0B389DA0F0F2DDF02BDDEC1B805475E4271AE6892347112F9D87FBB5A5CEA6D1108AB1C325C2534E0117DC31FFE6283CC5EE9733BC9BF7E5574EA817C1371E6424730013B233FA7F8A8E1F9ED5008D1479C2346A0D3560D3B2BB7733CC7AA5AA406BC769F1951B97B994BC202F469344F9D412C56F8A384547299196ED6C9BCDBD453FE346364BA30D3D07D450C3A4431723CB2984C9599210C58FD293B95638074728AB87A0613F01C21B61D4A57B6712A0DC5C44651A9766F31B5472F3F969FE636874AAABFB129633A3FF85519164607B200DB3ECC2AF97DF607E9E0BB293A457A24EF13B0A65C56E864C74C0EEA197EB7C5057F5D40A78A550CF2368A39C36DD6890E4A9575A0592974804C3BC00A0BEFF40BE4B5F350B00F3B87A52092A67BE7D3F477BAAC1CC1D2199B1032A86BD33D9BDC0DDAD0167812E995127037BD125299511A95DD90600E21969E8D7A4318AD6A082002C7F2CFAF834BCA2E8EDA79583DBA47102ACD698B157102A8EDCC782699481281702F516E369B83DD12418508852589C837389B7C2E331B9BDFF50486A0F4901525EDD62B57BE74B8352A514CBB6F42BA804CBEBDB06377DE6DE01FEC668B6D46849F648C12AD7706E743CC672FD9AA24A71576BAA78F41146B653279892F9FAC778D5CEF57A83E789B2D12AD008755A3A8B55DF06DC93CD7838E2DDDDE26D34C07530665F1B3988914FB9571F9004AC08AE27626FF24D19BEB19DD64CA5A8D62BC60655E0914C61605B61C8EB2B1AC806166E8B3773B28F6BAF9096FA3DAD83EB9785F111DD7DC97BEDB87F3576AF7AD5E9E0C7075AB59D740D56C734857881C1F3333CB7F4A3DB46FDA6F1E2EDFC1DDEEFCCF254895A79368887005F0A8060CD57C3EAB2081162021998E7367281A7AC7855FA4001DCBC3FE9FC47C6D46B7584512158039D3FAF589F0F9CEEF416D4C2F5427C776ED50530FDEBF2D6AD1E609310AE7C0EA5B84FE082B2ED0938BFF4CB915210F50B2054ECFBB925B87894B614B4B37DD8D7809988C8BE8AC5049F5B81F8B72A7E99E723E85E1494EF8D51579E910E25CB44D6130F7C5D194A824EBB0EB0A5CEFC9E5692FABAFA036E7C09063B46A738A5F42B8829A81F8EF8EA81EE21BFD76E3E640A3A215B7A3C9E277526EF6BCCDA1F981740BB8DC1B02DFC426FC0A614400A06D680E433F23BEA5CCFC6ACE3D20BDFE792A6491E7C5BC564E3CF56C480B78C3AC51B417C3F595C4B68D1F4E792DFA105FA179772536CABE7D125F585807D6574412E331710A5D31CF00000000000001A200000002540BE40100000002540BE401000000000000000D3130303030303030303030303102000000466D3F4EF74AE7682CFC54F5B2AB4B4810D4FA9474F639DC2D9177389B962181E69F3496A96B9B0E64A2A89FD189515E0B29542C0F72797A50976BD6D5954520F776C75B181D740000005054E4932E521FBE991FB121DCFFC21CEC6206C110BA4940F9579056AB012D1DCB0C4F06597B8E9F76AD95BFA04FB63EF5475C4D031932A705219CC526E91F5347E4813D7BF2E2EAA621A6F340D31B5AD7000000C0A154D8F81ADF411A1AF14BBAC004A02DA7ADBC9350A2165F19DC1B663E99216348AB003EF29A5051F8AE493DBEB982A5B29FA8DC68514AAE7576CED02401299364AE5355F0B9F89CE626A87FAA6F7EB2CDEAC72468984B227E95047DCDA4E070333DCFDD6CCD0F55CF414C9F8844F538D63103E2A3E000568FB3511FC4FBC2D05516649F565CB81D9E70F2170ED8F1914F097A4442CEFE8547CFC2DF8AE68CC54DE084E7AE81EBA7C5282E80274ECA53DE072A66BE467D290930099C1359FE5E0000027109DB2765BA702C137121314ACD6637156AC776497D3A89E549D5150F448B36D113C79F4F767C8B16C0C59DCF8B3279B5E70BEC320D55BF2D88B550DC416DC16407F480F9F459D903991A374D290B2BE8AD04860669DCACAF2047BA0E0C4358E8393E2F0FB8AD8BFDB8ADA961DD74E6E2891D10F504283F09D7B4D3341E6A6A258DDE8C59457F827F8ACD931282B845A119D484AD949EE22364C19E34D42A9D5CFCE8A916A615686BB36B71771754D266529EA69E95DAB3FFF551559CEDB06038161B7530E6E37F898ACF31803CFB2059FB205F33C7DE0031AAC8978D0E94FB027BFA47729885B6569818E1D4B8273D542FB65E1EAC5A0C64172F1C6D3EF04E026143685F50CB04811D910F4FF539C67DDF43B61E0BFF4407C63028F86A0ED52112A7D8B527D777C38BD0E038822960854E62656470E26250B3E55F80E29C9CE16ADA12FA3B4970EB09AEEC5090C559F393D216E1A9F51C2DCE6CB3D5DA7742D75DF484A12C7CEAB0F1025C8D7685AA601A45855E4A8868547D5675D5EA012CEC6F1CEC92A1D1E1B3523DEBBB2EE9A9F5237042292ADD749917DFE3AC8F0C84EA3A89E6BCC10E21616EC2BC67D56922174276DA7BE4CED4D56AF9AE9CA750BCD8ECCE5674BAC811B93D2125E12EFAA1BBDD9C8D9B5784883C35294572F9769519C67D72253EE15CF3E914DBDE20172A7DB1521AC52735D9ABAF2DF1FAABB0EF449E1096A301136EC2505735F33468A11D4E86519D39FE38A0FE7B126CCADFE5335D2B380D3C8E2C8A854360FB18F0A8DE964C6FD63BC698BEC189FA1AC64FF5C8822996CA2DA57A355048B5C11ECDB6BF42D771201A32E68887E738E6F2FD33939EFCF3A695884E76A92A9E47BD89986F5D000002CE562B60610872D789D65A8965381476244018AE8E0FFDBE80C787F69B816BCCBF25AB8D2BA4268EC3643D767C61AB571FF889DA4352BBF539B3AB7E51BBB276F061073ED8C4816D3B16AF58D15B25428F68F6FD626816682F29ADB99E38EAE56F6C8538794E5D766BCEEA5D6CF9C91DA359540EC456AE15AA85AFA9B7BAF1FE7B193A9B278A1FF2C200CAE8170F5E90EF4B6DB186C89118C6F64929A9487250BE320452E1A4632AC5378327CCF63FE664EFFF187C8CEE5E1A00AE8E4DAE422188F25682AF2C323DEB90B832970C945269AEB250021252BAAEFA1D1D3BFADB36878BD4B55195C90F3A3DD68F6B3281580521D6673FF37966FF91B3BE9900DE45ACA2B8CD98B9F6A1DB03C130E04C7D6F3896F7204E67B469CF4376C042D7A33E68CE12093453FD43F1BCD44A76C5A9347C924311B9CDD07095E5C18A6A5AE8E7191CD1A7F183107142D6329F6A0840BB344AB8599427A008611EFD8F59A97A79818E9AF45B4E5B5062D3600E71080ECE4C282864A19B055730E99FEB66A50229549779BF2A1C991E64C73691FAB90234854C0C4E2BF396B5D0178A6B7C77D5DA95A1569338383E023DDD1DF1CE94CA490E0827EBB51ECDCF8ECBA9FDE809DA68338742308A54300C80D29ED9D4177C82DB63F6892CADC4262DFCF1BB2B8B7D31741B91147E020F673778401DDC4354A1C097F6E3F8568E1CD2F6A010AFF0B7E03DA1C31E693C0CF73B35B6471F1C83BED094B4E34D750FE9E0D9D6AC206E20A5D1524392BCDC54EC92815ED7FA2D0869A77BB02AFD86721AFB197C4B300212439EDBEB085125090A076C11B809019F773C23108161AC2783BAFC777A04EADFB7FEDB651B9938C5A369153FD96FABBB5AA3972199432E75B70D1F37E4A1291A5F416851CA4DA0B993A5345EF99C3F97019F95ADD021F4977C83F281D3D606E2CB1F8B40F29DC753AB1491AA003B564CA443111ADAEE0E74CB3B37A3EBF98D4EC8E3BF8154097E7660568A33854C8E03";

        VoltTable vt = PrivateVoltTableFactory.createVoltTableFromBuffer(ByteBuffer.wrap(Encoder.hexDecode(hex)), false);

        VoltTable compressed = VoltTableUtil.toCompressedTable(vt);

        FastSerializer fs = new FastSerializer();
        compressed.writeExternal(fs);
        FastDeserializer fds = new FastDeserializer(fs.getBytes());
        VoltTable compressed2 = fds.readObject(VoltTable.class);
        VoltTable uncompressed = VoltTableUtil.uncompressTables(compressed2)[0];
        assertTrue(vt.equals(uncompressed));
    }
}
