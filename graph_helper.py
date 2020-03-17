import matplotlib.pyplot as plt
from matplotlib.widgets import TextBox

def make_graph(xs, x_label, ys, y_label, header, filename, des_pair_1=None, des_pair_2=None, des_pair_3=None, des_pair_4=None):
    figure = plt.figure(figsize=(14, 11))

    font_dict = {
        'fontsize': 21,
        'fontweight': 'bold',
     }


    ax = figure.add_subplot(111)
    plt.title(header, fontdict=font_dict)
    plt.subplots_adjust(top=0.75)
    plt.grid(b=True)
    plt.plot(xs, ys)
    plt.ylabel(y_label)
    plt.xticks(rotation=90)
    plt.xlabel(x_label)

    if des_pair_1 is not None:
        plt.text(des_pair_1['text_x'],  des_pair_1['text_y'], des_pair_1['text'], transform=ax.transAxes)
        axbox = plt.axes([0.1, 0.87, 0.2, 0.04])
        offset = 42 - 2*len(des_pair_1['value'])
        text1 = ''
        for i in range(0, offset):
            text1 += ' '
        text_box = TextBox(axbox, '', initial=text1 + des_pair_1['value'], color='orange', label_pad=0.005)
        text_box.disconnect_events()
    if des_pair_2 is not None:
        offset = 42 - 2*len(des_pair_2['value'])
        text2 = ''
        for i in range(0, offset):
            text2 += ' '
        plt.text(des_pair_2['text_x'],  des_pair_2['text_y'], des_pair_2['text'], transform=ax.transAxes)
        # plt.text(0.33, 1.27, des_pair_2['text'], transform=ax.transAxes)
        axbox = plt.axes([0.3, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text2 + des_pair_2['value'], color='blue')
        text_box.disconnect_events()
    if des_pair_3 is not None:
        offset = 42 - 2*len(des_pair_3['value'])
        text3 = ''
        for i in range(0, offset):
            text3 += ' '
        # plt.text(0.58, 1.27, des_pair_3['text'], transform=ax.transAxes)
        plt.text(des_pair_3['text_x'],  des_pair_3['text_y'], des_pair_3['text'], transform=ax.transAxes)
        axbox = plt.axes([0.5, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text3 + des_pair_3['value'], color='green')
        text_box.disconnect_events()
    if des_pair_4 is not None:
        offset = 42 - 2*len(des_pair_4['value'])
        text4 = ''
        for i in range(0, offset):
            text4 += ' '
        # plt.text(0.79, 1.27, des_pair_4['text'],  transform=ax.transAxes)
        plt.text(des_pair_4['text_x'],  des_pair_4['text_y'], des_pair_4['text'], transform=ax.transAxes)
        axbox = plt.axes([0.7, 0.87, 0.2, 0.04])
        text_box = TextBox(axbox, '', initial=text4 +des_pair_4['value'], color='red')
        text_box.disconnect_events()

    plt.savefig(filename)

if __name__ == '__main__':
    make_graph([1, 2, 3, 4], 'x', [1, 2, 3, 4], 'y', 'TEST', 'test')