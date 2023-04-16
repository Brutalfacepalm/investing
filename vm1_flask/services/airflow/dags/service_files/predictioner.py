import torch
from torch import nn
import numpy as np
import pandas as pd
import pickle


class PositionEmbeddingLearned(nn.Module):
    """
    Absolute pos embedding, learned.
    """

    def __init__(self, seq_len=27, num_pos_feats=512):
        super().__init__()
        self.embed = nn.Embedding(seq_len, num_pos_feats)
        self.reset_parameters()

    def reset_parameters(self):
        nn.init.uniform_(self.embed.weight)

    def forward(self, x):
        h, w = x.shape[1], x.shape[2]
        j = torch.arange(h, device=x.device)
        pos = self.embed(j)
        pos = pos.repeat(x.shape[0], 1, 1)
        return x + pos


class ExchangeTransformer(nn.Module):
    def __init__(self, device, augs, input_size, src_seq_len, dec_seq_len, batch_first,
                 n_encoder_layers, n_decoder_layers, n_heads, dim_val,
                 dropout_encoder, dropout_decoder, dropout_pos_enc,
                 dim_feedforward_encoder, dim_feedforward_decoder,
                 output_size, pred_len):
        super(ExchangeTransformer, self).__init__()

        self.output_size = output_size
        self.device = device
        self.input_size = input_size
        self.src_seq_len = src_seq_len - pred_len
        self.pred_len = pred_len
        self.augs = augs

        self.encoder_input_layer = nn.Linear(in_features=input_size, out_features=dim_val)
        # self.encoder_input_layer = nn.Sequential(nn.Linear(in_features=input_size, out_features=dim_val),
        #                                          nn.GELU(),
        #                                          nn.Dropout(0.2),
        #                                          nn.Linear(in_features=dim_val, out_features=dim_val),
        #  nn.Dropout(0.2),
        #  )
        self.encoder_input_layer.to(self.device)
        self.decoder_input_layer = nn.Linear(in_features=input_size, out_features=dim_val)
        # self.decoder_input_layer = nn.Sequential(nn.Linear(in_features=input_size, out_features=dim_val),
        #                                          nn.GELU(),
        #                                          nn.Dropout(0.2),
        #                                          nn.Linear(in_features=dim_val, out_features=dim_val),
        #                                         #  nn.Dropout(0.2),
        #                                          )
        self.decoder_input_layer.to(self.device)
        self.positional_encoding_layer = PositionEmbeddingLearned(seq_len=src_seq_len,
                                                                  num_pos_feats=dim_val)
        self.positional_encoding_layer.to(self.device)

        encoder_layer = nn.TransformerEncoderLayer(d_model=dim_val, nhead=n_heads,
                                                   dim_feedforward=dim_feedforward_encoder,
                                                   dropout=dropout_encoder, batch_first=batch_first,
                                                   activation='gelu',
                                                   norm_first=True
                                                   )
        self.encoder = nn.TransformerEncoder(encoder_layer=encoder_layer,
                                             num_layers=n_encoder_layers,
                                             norm=None)
        self.encoder.to(self.device)

        decoder_layer = nn.TransformerDecoderLayer(d_model=dim_val, nhead=n_heads,
                                                   dim_feedforward=dim_feedforward_decoder,
                                                   dropout=dropout_decoder, batch_first=batch_first,
                                                   activation='gelu',
                                                   norm_first=True
                                                   )
        self.decoder = nn.TransformerDecoder(decoder_layer=decoder_layer,
                                             num_layers=n_decoder_layers,
                                             norm=None)
        self.decoder.to(self.device)

        self.flatten = torch.nn.Sequential(torch.nn.Identity())

        self.src_mask = torch.triu(torch.ones((self.pred_len + 1, self.src_seq_len),
                                              device=self.device) * float('-inf'), diagonal=1).to(torch.bool)
        self.tgt_mask = torch.triu(torch.ones((self.pred_len + 1, self.pred_len + 1),
                                              device=self.device) * float('-inf'), diagonal=1).to(torch.bool)

    def forward(self, src):
        tgt = src[:, -self.pred_len - 1:, :].clone()
        src = src[:, :self.src_seq_len, :]

        src = self.encoder_input_layer(src)
        src = self.positional_encoding_layer(src)
        src = self.encoder(src)

        decoder_output = self.decoder_input_layer(tgt)

        decoder_output = self.decoder(
            tgt=decoder_output,
            memory=src,
            tgt_mask=self.tgt_mask,
            memory_mask=self.src_mask)

        return decoder_output[:, 1:]


class LinearHead(torch.nn.Module):
    def __init__(self, device, input_size, output_size=1):
        super(LinearHead, self).__init__()

        self.head = torch.nn.Sequential(torch.nn.Linear(input_size, input_size // 2),
                                        torch.nn.GELU(),
                                        torch.nn.Linear(input_size // 2, 4))

        self.head.to(device)

    def forward(self, x):
        out = self.head(x)

        return out


class PredictorPredict:
    def __init__(self,
                 device,
                 seq_len,
                 target_mode,
                 log=False,
                 ):

        self.seq_len = seq_len
        self.device = device
        self.log = log
        self.target_mode = target_mode

        self.shift_for_target = -9

        self.base_T_config = {'augs': True,
                              'input_size': 557,
                              'src_seq_len': self.seq_len,
                              'dec_seq_len': -self.shift_for_target,
                              'batch_first': True,
                              'n_encoder_layers': 4,
                              'n_decoder_layers': 4,
                              'n_heads': 32,
                              'dim_val': 512,
                              'dropout_encoder': 0.2,
                              'dropout_decoder': 0.2,
                              'dropout_pos_enc': 0.1,
                              'dim_feedforward_encoder': 2048,
                              'dim_feedforward_decoder': 2048,
                              'output_size': 512,
                              'pred_len': -1 * self.shift_for_target}

        self.model = self.build_model(self.device)

    def save_stat_dict(self, path):
        torch.save(self.model.state_dict(), path)

    def load_stat_dict(self, path):
        self.model.load_state_dict(torch.load(path, map_location=self.device), strict=False)

    def save_scaler(self, scaler, path):
        with open(path, 'wb') as f:
            pickle.dump(scaler, f)

    def load_scaler(self, path):
        with open(path, 'rb') as f:
            scaler = pickle.load(f)
        return scaler

    def build_model(self, device):
        model = nn.Sequential()
        model.add_module('embeddings', ExchangeTransformer(device, *self.base_T_config.values()))
        model.add_module('head', LinearHead(device, input_size=self.base_T_config['output_size'], output_size=4))

        return model

    def get_meta(self):
        embedding_params = sum([i.numel() for i in self.model.embeddings.parameters()])
        head_params = sum([i.numel() for i in self.model.head.parameters()])

        print(f'Embedding Parameters: {embedding_params}')
        print(f'Heads Parameters: {head_params}')
        print(f'Total Parameters: {embedding_params + head_params}')

    def load_state(self, path_model, path_scaler):
        self.load_stat_dict(path_model)
        self.scaler = self.load_scaler(path_scaler)

    def predict(self, data, path_model, path_scaler):
        self.load_state(path_model, path_scaler)

        results = {}
        assert data.shape[1] == 558, 'wrong shape'

        def get_future_datetimes(t):
            if pd.to_datetime(t).isoweekday() == 5:
                return pd.to_datetime(t) + pd.Timedelta(value=3, unit='days')
            else:
                return pd.to_datetime(t) + pd.Timedelta(value=1, unit='days')

        times = data.iloc[:, 0].apply(lambda x: get_future_datetimes(x))
        data_x = data.iloc[:, 1:]
        if data_x.shape[-1] == self.scaler.feature_names_in_.shape[0]:
            data_x.columns = self.scaler.feature_names_in_

        data_x = self.scaler.transform(data_x)

        self.model.eval()
        from time import time
        t0 = time()
        with torch.no_grad():
            for i in range(data_x.shape[0] - self.seq_len):
                i_data_x = torch.tensor(data_x[i: i + self.seq_len, :], dtype=torch.float32).unsqueeze(0)
                i_pred = self.model(i_data_x)
                i_pred = i_pred.cpu().detach().numpy()[:, -1, :].squeeze(0)
                print(i_pred)
                i_data_x_rev = self.scaler.inverse_transform(i_data_x[:, -1, :])[:, :4].squeeze(0)

                i_data_x_rev = np.exp(i_data_x_rev) if self.log else i_data_x_rev
                if self.target_mode == 'percentage':
                    results[times[i + self.seq_len]] = i_data_x_rev * (i_pred / 100 + 1)
                elif self.target_mode == 'abs':
                    results[times[i + self.seq_len]] = i_data_x_rev + i_pred
        print(time() - t0)
        return results