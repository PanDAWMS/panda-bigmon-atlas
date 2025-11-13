import {Component, Input, OnInit} from '@angular/core';


export type API_TYPES = 'gp' | 'ap' ;
@Component({
    selector: 'app-gp-api-instruction',
    templateUrl: './gp-api-instruction.component.html',
    styleUrls: ['./gp-api-instruction.component.css'],
    standalone: false
})
export class GpApiInstructionComponent implements OnInit {

  @Input() currentAPI: API_TYPES;

  constructor() { }
  cacheExample = {timestamp: '2021-02-25 17:43:04.415908+00:00', formats: [{output_format: 'DAOD_EGAM3', data: [{ami_tag: 'p3517', cache: '21.2.25.0,skim', containers: [{container: 'mc16_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.deriv.DAOD_EGAM3.e3601_s3126_r9364_p3517', events: '8161906', available_tags: 'p3954,p3947,p3517', version: '2', extensions_number: null, size: '978484229770', epoch_last_update_time: 1565887646, days_to_delete: -561}]}, {ami_tag: 'p3949', cache: '21.2.71.0,noskim', containers: [{container: 'mc16_13TeV.366140.Sh_224_NN30NNLO_eegamma_LO_pty_7_15.deriv.DAOD_EGAM3.e7006_s3126_r9364_p3949', events: '898000', available_tags: 'p4252,p3956,p3949', version: '2', extensions_number: null, size: '71375202173', epoch_last_update_time: 1568198063, days_to_delete: -534}]}]}]};

  prOutputExample = {    0: [
        {
            task: 39442658,
            status: 'finished',
            outputs: [
                'valid2.900050.PG_single_photon_egammaET.merge.NTUP_PHYSVAL.e8514_s4323_s4324_r15565_p6223_p6224_p6225_tid39442658_00'
            ],
            ami_tag: 'p6225'
        }
    ],
    1: [
        {
            task: 39442670,
            status: 'finished',
            outputs: [
                'valid2.900333.PG_single_electron_egammaET.merge.NTUP_PHYSVAL.e8514_s4323_s4324_r15565_p6223_p6224_p6225_tid39442670_00'
            ],
            ami_tag: 'p6225'
        }
    ], };
  htOutputExample = {
    "hashtag": "testhashtag",
    "tasks": [
        10328174,
        10329416,
        10330568,
        10332424,
        10334020]
  };
  protected socOutputExample  =
    {
    "hashtag": "25p0p42_MC23_DAOD_Production",
    "containers": {
        "ready": [
            "mc23_13p6TeV.700893.Sh_2214_Ztautau_mZ_105_ECMS_CFilterBVeto.deriv.DAOD_PHYSLITE.e8514_s4162_r15540_p7017",
            "mc23_13p6TeV.525359.MGPy8EG_A14NNPDF30NLO_ttHH.deriv.DAOD_PHYS.e8532_s4162_r14622_p7017",
            "mc23_13p6TeV.601707.PhPy8EG_dijet_JZ8incl.deriv.DAOD_PHYSLITE.e8514_s4162_r14622_p7017",
            "mc23_13p6TeV.701041.Sh_llll_MUQ20.deriv.DAOD_PHYSLITE.e8582_a934_r16083_p7017",
            "mc23_13p6TeV.701061.Sh_lvvv_MUQ20.deriv.DAOD_PHYSLITE.e8582_a911_r15530_p7017"],
      "upcoming": [
            "mc23_13p6TeV.700819.Sh_2214_jj_JZ4.deriv.DAOD_PHYSLITE.e8514_s4159_r15224_p7017",
            "mc23_13p6TeV.701281.Sh_22_ttll_Qcut40_3LFilter.deriv.DAOD_PHYS.e8532_a934_r16083_p7017",
            "mc23_13p6TeV.700994.Sh_2214_ttW_muQ025_2LFilter.deriv.DAOD_PHYS.e8532_a911_r15224_p7017",
            "mc23_13p6TeV.700788.Sh_2214_Zee_maxHTpTV2_CVetoBVeto.deriv.DAOD_PHYS.e8514_s4162_r15540_p7017"]
  }};
  ngOnInit(): void {
  }

}
