#ifndef TOUCH_DEFS_H
#define TOUCH_DEFS_H

enum TOUCH_LOCATION { NEURON_ID, SECTION_ID, SEGMENT_ID };

struct Touch {
    int getPreNeuronID(){
        return pre_synapse_ids[NEURON_ID];
    }
    int getPostNeuronID(){
        return post_synapse_ids[NEURON_ID];
    }

    int pre_synapse_ids[3];
    int post_synapse_ids[3];
    int branch;
    float distance_soma;
    float pre_offset, post_offset;
};


#endif // TOUCH_DEFS_H
